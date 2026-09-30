defmodule RegistryRegistrationIntentTest do
  use ExUnit.Case

  setup do
    suffix = System.unique_integer([:positive])
    names = for name <- [:a, :b, :c], do: :"intent_#{name}_#{suffix}"

    for name <- names do
      start_supervised!(
        {Horde.Registry,
         name: name, keys: :unique, members: names, delta_crdt_options: [sync_interval: 10]}
      )
    end

    await(fn -> Enum.all?(names, &(length(Horde.Cluster.members(&1)) == 3)) end)

    [
      names: names,
      first: Enum.at(names, 0),
      second: Enum.at(names, 1),
      owner_registry: Enum.at(names, 2)
    ]
  end

  test "a live owner's latest value returns after its member rejoins", context do
    owner = owner(context.owner_registry, :name, :initial)
    await_names(context.names, [{owner, :initial}])

    assert {:updated, :initial} =
             invoke(owner, fn ->
               Horde.Registry.update_value(context.owner_registry, :name, fn _ -> :updated end)
             end)

    await_names(context.names, [{owner, :updated}])
    exclude_owner(context)
    assert Process.alive?(owner)
    rejoin_owner(context)
    await_names(context.names, [{owner, :updated}])
  end

  test "new registration intent stays unpublished while its member is excluded", context do
    exclude_owner(context)
    owner = owner(context.owner_registry, :name, :pending)
    :ok = Horde.Cluster.set_members(context.owner_registry, [context.first, context.second])

    for registry <- context.names do
      assert nil == DeltaCrdt.get(crdt(registry), {:key, :name})
      assert [] == Horde.Registry.lookup(registry, :name)
    end

    rejoin_owner(context)
    await_names(context.names, [{owner, :pending}])
  end

  test "explicit unregister during exclusion retires intent permanently", context do
    owner = owner(context.owner_registry, :name, :value)
    await_names(context.names, [{owner, :value}])
    exclude_owner(context)
    assert :ok = invoke(owner, fn -> Horde.Registry.unregister(context.owner_registry, :name) end)
    assert %{} == :sys.get_state(context.owner_registry).local_registrations
    rejoin_owner(context)
    assert_empty_names(context.names)
    assert Process.alive?(owner)
  end

  test "owner death during exclusion retires intent permanently", context do
    owner = owner(context.owner_registry, :name, :value)
    await_names(context.names, [{owner, :value}])
    exclude_owner(context)
    Process.exit(owner, :kill)
    await(fn -> :sys.get_state(context.owner_registry).local_registrations == %{} end)
    rejoin_owner(context)
    assert_empty_names(context.names)
  end

  test "a conflict loser that traps exits cannot resurrect after the winner unregisters",
       context do
    loser = owner(context.owner_registry, :name, :old)
    await_names(context.names, [{loser, :old}])
    exclude_owner(context)
    winner = owner(context.first, :name, :winner)
    await_names(context.names, [{winner, :winner}])
    assert_receive {:owner_exit, ^loser, {:name_conflict, {:name, :old}, _, ^winner}}, 1_000
    assert %{} == :sys.get_state(context.owner_registry).local_registrations
    assert Process.alive?(loser)
    assert :ok = invoke(winner, fn -> Horde.Registry.unregister(context.first, :name) end)
    await_names(context.names, [])
    rejoin_owner(context)
    assert_empty_names(context.names)
    assert Process.alive?(loser)
  end

  test "a queued membership removal cannot delete a rejoined member's claim", context do
    owner = owner(context.owner_registry, :name, :value)
    await_names(context.names, [{owner, :value}])
    registry = context.owner_registry
    member = {registry, node()}
    :ok = :sys.suspend(registry)

    try do
      :ok = Horde.Cluster.set_members(context.first, [context.first, context.second])
      await(fn -> DeltaCrdt.get(crdt(registry), {:member, member}) == nil end)
      :ok = Horde.Cluster.set_members(context.second, context.names)
      await(fn -> DeltaCrdt.get(crdt(registry), {:member, member}) == 1 end)
      :ok = :sys.resume(registry)
      await_names(context.names, [{owner, :value}])
      assert Process.alive?(owner)
    after
      :sys.resume(registry)
    end
  end

  test "restoration cannot replace an already resolved winner", context do
    loser = owner(context.owner_registry, :name, :old)
    await_names(context.names, [{loser, :old}])
    exclude_owner(context)
    registry = context.owner_registry
    :ok = :sys.suspend(registry)

    try do
      winner = owner(context.first, :name, :winner)
      await(fn -> match?({_, ^winner, :winner}, DeltaCrdt.get(crdt(registry), {:key, :name})) end)
      :ok = Horde.Cluster.set_members(context.second, context.names)
      await(fn -> DeltaCrdt.get(crdt(registry), {:member, {registry, node()}}) == 1 end)
      :ok = :sys.resume(registry)
      await_names(context.names, [{winner, :winner}])
      assert_receive {:owner_exit, ^loser, {:name_conflict, {:name, :old}, _, ^winner}}, 1_000
      assert %{} == :sys.get_state(registry).local_registrations
    after
      :sys.resume(registry)
    end
  end

  defp owner(registry, key, value) do
    observer = self()

    owner =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        result = Horde.Registry.register(registry, key, value)
        send(observer, {:registered, self(), result})
        owner_loop(observer)
      end)

    on_exit(fn -> Process.exit(owner, :kill) end)
    assert_receive {:registered, ^owner, {:ok, _}}, 1_000
    owner
  end

  defp owner_loop(observer) do
    receive do
      {:invoke, caller, reference, operation} ->
        send(caller, {reference, operation.()})

      {:EXIT, _from, reason} ->
        send(observer, {:owner_exit, self(), reason})
    end

    owner_loop(observer)
  end

  defp invoke(owner, operation) do
    reference = make_ref()
    send(owner, {:invoke, self(), reference, operation})
    assert_receive {^reference, result}, 1_000
    result
  end

  defp exclude_owner(context) do
    :ok = Horde.Cluster.set_members(context.first, [context.first, context.second])
    await(fn -> Enum.all?(context.names, &(length(Horde.Cluster.members(&1)) == 2)) end)
    await_names(context.names, [])
  end

  defp rejoin_owner(context) do
    :ok = Horde.Cluster.set_members(context.second, context.names)
    await(fn -> Enum.all?(context.names, &(length(Horde.Cluster.members(&1)) == 3)) end)
  end

  defp assert_empty_names(names) do
    for registry <- names do
      :sys.get_state(registry)
      assert nil == DeltaCrdt.get(crdt(registry), {:key, :name})
      assert [] == Horde.Registry.lookup(registry, :name)
    end
  end

  defp await_names(names, expected) do
    await(fn -> Enum.all?(names, &(Horde.Registry.lookup(&1, :name) == expected)) end)
  end

  defp crdt(registry), do: :"#{registry}.Crdt"
  defp await(check, attempts \\ 200)
  defp await(_check, 0), do: flunk("registry did not reach the expected state")

  defp await(check, attempts) do
    unless check.() do
      Process.sleep(10)
      await(check, attempts - 1)
    end
  end
end
