defmodule HashRing do
  # should be in the range 100..200, helps distribute evenly 
  # with small amounts of nodes.
  @points_generated 100

  use GenServer

  def start_link(initial_topology) do
    GenServer.start_link(__MODULE__, initial_topology, name: __MODULE__)
  end

  def whereis(item) do
    GenServer.call(__MODULE__, {:whereis, item})
  end

  def add(new_node) do
    GenServer.call(__MODULE__, {:addnode, new_node})
  end

  def remove(old_node) do
    GenServer.call(__MODULE__, {:rmanode, old_node})
  end

  def init(initial_topology) do
    tree = generate_points(initial_topology)

    {:ok, tree}
  end

  def handle_call({:whereis, item}, _from, tree) do
    location = route(get_point(item), tree)

    {:reply, location, tree}
  end

  def handle_call({:addnode, new_node}, _from, tree) do
    {:reply, :added, generate_points(new_node, tree)}
  end

  def handle_call({:rmanode, old_node}, _from, tree) do
    {:reply, :removed, remove_points(old_node, tree) |> :gb_trees.balance()}
  end

  defp generate_points([head | tail]) do
    tree = :gb_trees.empty()

    generate_points(head, tail, tree)
  end

  defp generate_points([]) do
    :gb_trees.empty()
  end

  defp generate_points(curr, tree) do
    spread_distribution(curr, tree, 0)
  end

  defp generate_points(curr, [], tree) do
    generate_points(curr, tree)
  end

  defp generate_points(curr, [head | tail], tree) do
    generate_points(head, tail, generate_points(curr, tree))
  end

  defp spread_distribution(curr, tree, acc) when acc < @points_generated do
    spread_distribution(
      curr,
      :gb_trees.insert(get_point(get_name(curr, acc)), curr, tree),
      acc + 1
    )
  end

  defp spread_distribution(_, tree, _), do: tree

  defp get_name(node_name, number) do
    Atom.to_string(node_name) <> Integer.to_string(number)
  end

  defp remove_points(old_node, tree) do
    remove_point(old_node, tree, 0)
  end

  defp remove_point(old_node, tree, acc) when acc < @points_generated do
    remove_point(
      old_node,
      :gb_trees.delete(get_point(get_name(old_node, acc)), tree),
      acc + 1
    )
  end

  defp remove_point(_, tree, _), do: tree

  # tl;dr we are going to use a `gb_tree` since that's
  # basically the fastest we can get... yeah, sad life...
  # also, we aren't supposed to pattern match them: too bad
  def route(item, {key, value, _, _}) when item == key, do: value
  def route(_, nil), do: nil
  def route(_, {_, nil}), do: nil

  def route(item, {key, _, _, bigger}) when item > key do
    route(item, bigger)
  end

  def route(item, {key, value, smaller, _}) when item < key do
    # it may be this key, or something in the `smaller` tree
    route(item, smaller) || value
  end

  def route(item, {_, tree} = real_tree) do
    route(item, tree) || elem(:gb_trees.smallest(real_tree), 1)
  end

  def get_point(value) when is_atom(value) do
    value |> Atom.to_string |> get_point
  end

  def get_point(value) when is_integer(value) do
    value |> Integer.to_string |> get_point
  end

  def get_point(value) when is_binary(value) do
    << result :: 32, _ :: binary >> = :erlang.md5(value)

    result
  end

  def get_point(value) do
    :erlang.term_to_binary(value)
    |> get_point
  end
end
