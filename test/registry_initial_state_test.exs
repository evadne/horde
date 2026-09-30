defmodule RegistryInitialStateTest do
  use ExUnit.Case

  test "initialisation materialises membership and registrations received before it starts" do
    suffix = System.unique_integer([:positive])
    registry = :"initial_state_#{suffix}"
    peer = :"initial_peer_#{suffix}"
    crdt = :"#{registry}.Crdt"
    members = [{registry, node()}, {peer, node()}]
    owner = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(owner, :kill) end)

    start_supervised!(
      {DeltaCrdt,
       crdt: Horde.RegistryCrdt, name: crdt, on_diffs: {Horde.RegistryImpl, :on_diffs, [registry]}}
    )

    # CRDT replication can arrive between the CRDT child's start and the
    # registry implementation's start. Its notification has no recipient yet.
    assert Process.whereis(registry) == nil

    preexisting = %{
      {:member, {registry, node()}} => 1,
      {:member, {peer, node()}} => 1,
      {:key, :existing} => {{peer, node()}, owner, :value},
      {:registry, :existing_meta} => :metadata
    }

    ^crdt = DeltaCrdt.merge(crdt, preexisting)
    assert DeltaCrdt.to_map(crdt) == preexisting

    start_supervised!({Horde.RegistryImpl, name: registry, members: [registry]})

    state = :sys.get_state(registry)
    assert state.members == MapSet.new(members)
    assert Enum.sort(Horde.Cluster.members(registry)) == Enum.sort(members)

    assert Enum.sort(:ets.tab2list(state.members_ets_table)) ==
             Enum.sort(Enum.map(members, &{&1, 1}))

    assert Horde.Registry.lookup(registry, :existing) == [{owner, :value}]
    assert Horde.Registry.meta(registry, :existing_meta) == {:ok, :metadata}
    assert DeltaCrdt.to_map(crdt) == preexisting
  end
end
