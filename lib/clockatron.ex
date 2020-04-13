defmodule Clockatron.App do
  use Application

  def start(_type, _args) do
    children = [
      {HashRing, Node.list() ++ [node()]},
      {Clockatron.Listener, :ok},
      {Clockatron, :ok}
    ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Clockatron.Supervisor
    )
  end
end

defmodule Clockatron.Listener do
  use GenServer

  def start_link(:ok) do
    GenServer.start_link(__MODULE__, :ok)
  end

  def init(:ok) do
    :net_kernel.monitor_nodes(true)

    {:ok, :the_state_does_not_matter}
  end

  def handle_info({:nodeup, new_node}, state) do
    HashRing.add(new_node)
    Clockatron.new_node()
    Clockatron.send_other_procs(new_node)

    {:noreply, state}
  end

  def handle_info({:nodedown, old_node}, state) do
    HashRing.remove(old_node)

    GenServer.call(Clockatron, :dead_node, :infinity)

    {:noreply, state}
  end
end

defmodule Clockatron do
  @start_cd 5_000
  use GenServer
  require Logger

  def start(name, module, args) do
    start(name, module, :init, args)
  end

  def start(name, module, function, args) do
    where = HashRing.whereis(name)
    GenServer.call(
      {__MODULE__, where},
      {:start, name, {module, function, args}}
    )
  end

  def handoff(name, other_node) do
    GenServer.call(__MODULE__, {:move, name, other_node})
  end

  def pidof(name) do
    where = HashRing.whereis(name)
    GenServer.call({__MODULE__,  where}, {:pidof, name})
  end

  def new_node() do
    GenServer.call(__MODULE__, :new_node, :infinity)
  end

  def graceful_shutdown() do
    GenServer.call(__MODULE__, :graceful_shutdown, :infinity)
  end

  def send_other_procs(to_node) do
    GenServer.call(__MODULE__, {:send_other_procs, to_node})
  end

  def start_link(:ok) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    other_procs = :ets.new(:other_processes, [:set])
    procs = :ets.new(:processes, [:set])
    mons = :ets.new(:monitors, [:set])

    {:ok, {other_procs, procs, mons}}
  end

  def handle_call(
      {:start, name, {module, function, args} = start},
      _from,
      {others, procs, mons}
  ) do
    pid = :proc_lib.spawn(fn ->
      {:ok, start_state} = Kernel.apply(module, function, args)
      :gen_server.enter_loop(module, [], start_state)
    end)

    mid = :erlang.monitor(:process, pid)

    :ets.insert(procs, {name, pid, mid, start})
    :ets.insert(mons, {mid, name})

    :gen_server.multi_call(
      Node.list(),
      Clockatron,
      {:new_process, {name, start}}
    )

    {:reply, :ok, {others, procs, mons}}
  end

  def handle_call({:move, name, other_node}, _from, {o, procs, mons}) do
    case :ets.lookup(procs, name) do
      [{^name, pid, mid, start}] ->
        GenServer.call(pid, :pause)
        state = :sys.get_state(pid)

        :ets.delete(procs, name)
        :ets.delete(mons, mid)

        :erlang.exit(pid, :pausing)

        GenServer.call(
          {Clockatron, other_node},
          {:resume, name, state, start}
        )

        :ets.insert(o, {name, start})

        {:reply, :ok, {o, procs, mons}}
      [] ->
        {:reply, {:error, :noproc}, {o, procs, mons}}
    end
  end

  def handle_call({:resume, name, state, start}, _from, {o, procs, mons}) do
    {module, function, _} = start
    pid = :proc_lib.spawn(
      fn ->
        {:ok, new_state} = Kernel.apply(module, function, [{:resume, state}])

        :gen_server.enter_loop(module, [], new_state)
      end
    )

    mid = :erlang.monitor(:process, pid)

    :ets.delete(o, name)
    :ets.insert(procs, {name, pid, mid, start})
    :ets.insert(mons, {mid, name})

    {:reply, :ok, {o, procs, mons}}
  end

  def handle_call({:pidof, name}, _from, {o, procs, mons}) do
    case :ets.lookup(procs, name) do
      [{_, pid, _, _}] -> {:reply, pid, {o, procs, mons}}
      [] -> {:reply, :noproc, {o, procs, mons}}
    end
  end

  def handle_call(:new_node, _from, {o, procs, mons}) do
    migrating =
      :ets.tab2list(procs)
      |> Enum.map(fn {name, _, _, _} = process ->
        {process, HashRing.whereis(name)}
      end)
      |> Enum.drop_while(fn {{_, _, _, _}, new_node} ->
        new_node == node()
      end)

    # this is slow but :/
    for {{name, _, _, _}, where} <- migrating do
      # can't call a genserver from itself :(
      handle_call({:move, name, where}, 1, {o, procs, mons})
    end

    {:reply, :ok, {o, procs, mons}}
  end

  def handle_call(:graceful_shutdown, _from, {o, procs, mons}) do
    HashRing.remove(node())

    for {name, _, _, _} <- :ets.tab2list(procs) do
      where = HashRing.whereis(name)
      handle_call({:move, name, where}, 1, {o, procs, mons})
    end

    case Node.stop() do
      {:error, :not_allowed} ->
        :init.stop()
      _ -> nil
    end

    {:reply, :ok, {o, procs, mons}}
  end

  def handle_call({:new_process, info}, _from, {others, procs, mons}) do
    :ets.insert(others, info)

    {:reply, :ok, {others, procs, mons}}
  end

  def handle_call(:dead_node, _from, {others, procs, mons}) do
    for {name, start} <- :ets.tab2list(others) do
      if HashRing.whereis(name) == node() do
        send self(), {:start, name, start}
        :ets.delete(others, name)
      end
    end

    {:reply, :ok, {others, procs, mons}}
  end

  def handle_call(
      {:send_other_procs, to_node},
      _from,
      {others, procs, mons}
  ) do
    total_procs = for {name, _, _, start} <- :ets.tab2list(procs) do
      {name, start}
    end

    total_procs = total_procs ++ :ets.tab2list(others)
    # otherwise we will deadlock...
    send {Clockatron, to_node}, {:process_others, total_procs}

    {:reply, :ok, {others, procs, mons}}
  end

  def handle_info({:process_others, total_procs}, {others, ps, ms}) do
    total_procs = Enum.drop_while(total_procs, fn {name, _} ->
      HashRing.whereis(name) == node()
    end)

    for proc <- total_procs do
      :ets.insert(others, proc)
    end

    {:noreply, {others, ps, ms}}
  end

  def handle_info({:DOWN, mid, :process, _pid, _error}, {o, procs, mons}) do
    case :ets.lookup(mons, mid) do
      [{^mid, name}] ->
        :ets.delete(mons, mid)

        {module, function, args} = :ets.lookup_element(procs, name, 4)

        Process.send_after(
          self(),
          {:start, name, {module, function, args}},
          @start_cd
        )
      [] -> nil
    end

    {:noreply, {o, procs, mons}}
  end

  def handle_info(
    {:start, name, {module, function, args}},
    {o, procs, mons}
  ) do
    pid = :proc_lib.spawn(fn ->
      {:ok, start_state} = Kernel.apply(module, function, args)
      :gen_server.enter_loop(module, [], start_state)
    end)

    mid = :erlang.monitor(:process, pid)

    :ets.insert(procs, {name, pid, mid, {module, function, args}})
    :ets.insert(mons, {mid, name})

    {:noreply, {o, procs, mons}}
  end

  def handle_info({:new_process, {name, start}}, state) do
    handle_call({:new_process, {name, start}}, 1, state)

    {:noreply, state}
  end

  def handle_info(_unhandled_msg, state) do
    {:noreply, state}
  end
end
