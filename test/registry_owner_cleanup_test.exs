defmodule RegistryOwnerCleanupTest do
  use ExUnit.Case

  setup do
    name = :"owner_cleanup_#{System.unique_integer([:positive])}"
    start_supervised!({Horde.Registry, name: name, keys: :unique})
    [registry: name, crdt: :"#{name}.Crdt"]
  end

  test "an old owner's exit cannot delete a winner whose CRDT notification is queued", %{
    registry: registry,
    crdt: crdt
  } do
    old = spawn(fn -> Process.sleep(:infinity) end)
    winner = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      Process.exit(old, :kill)
      Process.exit(winner, :kill)
    end)

    member = {registry, node()}
    DeltaCrdt.put(crdt, {:key, :name}, {member, old, :old})
    await(fn -> Horde.Registry.lookup(registry, :name) == [{old, :old}] end)
    :ok = :sys.suspend(registry)

    try do
      Process.exit(old, :kill)

      await(fn ->
        {:messages, messages} = Process.info(Process.whereis(registry), :messages)
        Enum.any?(messages, &match?({:EXIT, ^old, :killed}, &1))
      end)

      # The CRDT has selected a replacement; the registry still has the old ETS
      # view and will process that owner's EXIT before this update notification.
      DeltaCrdt.put(crdt, {:key, :name}, {member, winner, :winner})
      :ok = :sys.resume(registry)
      :sys.get_state(registry)
      assert {^member, ^winner, :winner} = DeltaCrdt.get(crdt, {:key, :name})
      await(fn -> Horde.Registry.lookup(registry, :name) == [{winner, :winner}] end)
      assert Process.alive?(winner)
    after
      :sys.resume(registry)
    end
  end

  test "unregister cannot remove another process's registration", %{
    registry: registry,
    crdt: crdt
  } do
    holder = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(holder, :kill) end)
    member = {registry, node()}
    DeltaCrdt.put(crdt, {:key, :name}, {member, holder, :value})
    await(fn -> Horde.Registry.lookup(registry, :name) == [{holder, :value}] end)
    assert :ok = Horde.Registry.unregister(registry, :name)
    assert {^member, ^holder, :value} = DeltaCrdt.get(crdt, {:key, :name})
    assert [{^holder, :value}] = Horde.Registry.lookup(registry, :name)
  end

  test "owner cleanup retains concurrent registrations in the standard AWLWWMap format" do
    alias DeltaCrdt.AWLWWMap
    key = {:key, :name}
    old = self()
    winner = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(winner, :kill) end)
    left = AWLWWMap.add(key, {{:registry, node()}, old, :old}, 1, AWLWWMap.new())
    right = AWLWWMap.add(key, {{:registry, node()}, winner, :new}, 2, AWLWWMap.new())
    merged = AWLWWMap.join(left, right, [key])
    removal = Horde.RegistryCrdt.remove_owner(key, old, 1, merged)
    assert %AWLWWMap{} = removal

    # Both a peer which already saw the winner and a peer which receives it
    # after cleanup must retain it, using the unmodified library's merge code.
    for state <- [merged, left] do
      result = state |> AWLWWMap.join(removal, [key]) |> AWLWWMap.join(right, [key])
      assert %{^key => {_, ^winner, :new}} = AWLWWMap.read(result)
    end
  end

  defp await(check, attempts \\ 100)
  defp await(_check, 0), do: flunk("registry did not reach the expected state")

  defp await(check, attempts) do
    unless check.() do
      Process.sleep(10)
      await(check, attempts - 1)
    end
  end
end
