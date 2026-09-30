# Eventual Consistency

Horde uses a CRDT to sync data between nodes. This means that two nodes in your cluster can have a different view of the data, with differences being merged as the nodes sync with each other. We call this "eventually consistent", and the result is that we have to deal with merge conflicts and race conditions. Horde's CRDT automatically resolves conflicts, but we still have to deal with the after-effects.

## Horde.DynamicSupervisor merge conflict

It is unlikely, but possible, that Horde.DynamicSupervisor will start the same process on two separate nodes.

This can happen:
- if using a custom distribution strategy, or
- when a node dies and not all nodes have the same view of the cluster, or
- if there is a network partition.

After a partition heals, `Horde.Registry` resolves competing registrations and sends an exit signal to the losing named process. `Horde.DynamicSupervisor` does not independently choose which live copy to terminate: the two components could otherwise choose opposite winners and kill both. Since Horde 0.6, fresh child starts ignore the caller's child-specification ID. The private logical ID retained across takeover tracks recovery; it is not an application uniqueness guarantee. Unnamed copies can remain alive after healing.

A member retains its local supervision intent even if the replicated record temporarily selects another copy. Missing records, or records whose replacement owner has become unreachable without any valid witness, can be restored from a live local copy. A reachable competing owner is not continually overwritten. This preserves recovery information while allowing Registry conflict handling to retire the losing named worker.

## Horde.Registry merge conflict

When processes on two different nodes have claimed the same name, this will generate a conflict in Horde.Registry. The CRDT resolves the conflict and Horde.Registry sends an exit signal to the process that lost the conflict. This can be a common occurrence.

Unless this message is handled, it will cause the process to exit. Handling the exit message isn't strictly necessary, because we usually want the process to exit in this case. If for some reason you want to handle the message, simply trap exits in the `init/1` callback and handle the message as follows:

```elixir
def init(arg) do
  Process.flag(:trap_exit, true)
  {:ok, state}
end

def handle_info({:EXIT, _from, {:name_conflict, {key, value}, registry, pid}}, state) do
  # handle the message, add some logging perhaps, and probably stop the GenServer.
  {:stop, :normal, state}
end
```

Note that, unless your process has `restart: :transient` in its child spec and you have handled the message to shut down the process cleanly, it will be restarted by its supervisor.

If a recovery attempt encounters the existing registered process, return the ordinary start error unchanged:

```elixir
def start_link(arg) do
  GenServer.start_link(__MODULE__, arg, name: via_tuple(arg))
end
```

`{:error, {:already_started, pid}}` keeps the recovery obligation pending; Horde does not adopt that PID into another local supervisor. Returning `:ignore` deliberately declines the child and retires that recovery obligation. Do not translate a duplicate error into `:ignore` if recovery must remain possible after the current owner disappears.

If you call `Horde.Registry.register/3` inside `init/1`, return a failed start when the name is already registered:

```elixir
def init(arg) do
  case Horde.Registry.register(:my_registry, "key", "value") do
    {:ok, _pid} ->
      {:ok, arg}

    {:error, {:already_registered, pid}} ->
      {:stop, {:already_registered, pid}}
  end
end
```
