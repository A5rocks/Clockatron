# Clockatron

Want to know what this does? Read the source!

tl;dr a knockoff bitwalker/swarm, except not as good.


Usage:
`Clockatron.start(id, module, [args])`
Note that this only works with GenServers, which should implement `handle_call(:pause, _from, state)` to close any external resources. The args will be passed into `init`, so `Clockatron.start("hello", YourModule, [1, 2])` will call `YourModule.init(1, 2)`. When your module gets resumed, Clockatron will call `YourModule.init({:resume, previous_state})`

`Clockatron.graceful_shutdown()`
Send every running process to another node, and then stop.

`Clockatron.handoff(id, other_node)`
Pauses the module and sends it to the other node to be resumed.

**NOTE THAT STATE WILL BE LOST WHEN A NODE DIES**