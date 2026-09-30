defmodule Horde.DynamicSupervisorRecoveryTest do
  use ExUnit.Case

  alias Horde.RecoveryTest.{FirstMemberDistribution, GuardedWorker, NamedWorker, Support}
  alias Horde.TestCluster, as: Cluster

  test "failed takeover retains its specification and recovers after the owner subsequently dies" do
    {first, owner, owner_root, worker_name, gate} = local_pair()
    spec = %{id: :named, start: {NamedWorker, :start_link, [{worker_name, self(), gate}]}}
    assert {:ok, original} = Horde.DynamicSupervisor.start_child(owner, spec)
    join_pair(first, owner, original)

    # Inject the observation, not a replicated death claim. The owner really
    # remains alive, so starting its named replacement must fail.
    ref = Map.fetch!(:sys.get_state(first).name_to_supervisor_ref, {owner, node()})
    send(first, {:DOWN, ref, :process, Process.whereis(owner), :noconnection})

    await(fn -> map_size(Support.snapshot(first).pending) == 1 end)

    for name <- [first, owner] do
      snapshot = Support.snapshot(name)
      assert [{_, {{^owner, _}, _, ^original}}] = snapshot.records
      assert snapshot.replicated[{:member_node_info, {owner, node()}}].status == :alive
    end

    Process.exit(owner_root, :kill)
    await(fn -> not Process.alive?(original) end)

    await(fn ->
      case Horde.DynamicSupervisor.local_processes(first, 1_000) do
        [{_, replacement, _}] -> replacement != original and Process.alive?(replacement)
        _ -> false
      end
    end)

    assert Support.snapshot(first).pending == %{}
    assert [{_, replacement, _}] = Horde.DynamicSupervisor.local_processes(first)
    assert Process.whereis(worker_name) == replacement
  end

  test "a live local copy restores ownership when the recorded replacement dies before cleanup" do
    suffix = System.unique_integer([:positive])
    names = [a, b, c] = for letter <- ~w(a b c), do: :"orphan_#{letter}_#{suffix}"
    dead_owner_root = start_horde(a, [a])
    start_horde(b, [b])
    start_horde(c, [c])
    gate = start_supervised!({Agent, fn -> :run end})
    name = :"orphan_worker_#{suffix}"
    spec = %{id: :original, start: {NamedWorker, :start_link, [{name, self(), gate}]}}
    assert {:ok, original} = Horde.DynamicSupervisor.start_child(c, spec)
    for horde <- names, do: Horde.Cluster.set_members(horde, names)
    await(fn -> Enum.all?(names, &(map_size(:sys.get_state(&1).name_to_supervisor_ref) == 3)) end)
    [{id, ^original, child_spec}] = Horde.DynamicSupervisor.local_processes(c)

    # Model the independently selected replacement contribution. Registry still
    # names C's live original; A then disappears without sending child cleanup.
    replacement_record = {{a, node()}, child_spec, Process.whereis(a)}
    DeltaCrdt.put(:"#{a}.Crdt", {:process, id}, replacement_record)

    await(fn ->
      Enum.all?(names, &(Support.snapshot(&1).records == [{id, replacement_record}]))
    end)

    Process.exit(dead_owner_root, :kill)

    await(fn ->
      Enum.all?([b, c], fn horde ->
        match?([{^id, {{^c, _}, _, ^original}}], Support.snapshot(horde).records)
      end)
    end)

    assert Process.whereis(name) == original
    assert Horde.DynamicSupervisor.local_processes(b) == []
    assert Support.snapshot(b).pending == %{}
  end

  test "a deliberate ignore retires the recovery obligation" do
    {first, owner, owner_root, worker_name, gate} = local_pair()
    spec = %{id: :ignored, start: {NamedWorker, :start_link, [{worker_name, self(), gate}]}}
    assert {:ok, original} = Horde.DynamicSupervisor.start_child(owner, spec)
    join_pair(first, owner, original)
    Agent.update(gate, fn _ -> :ignore end)
    Process.exit(owner_root, :kill)

    await(fn ->
      snapshot = Support.snapshot(first)
      not Process.alive?(original) and snapshot.records == [] and snapshot.pending == %{}
    end)

    assert Horde.DynamicSupervisor.local_processes(first) == []
  end

  test "local process diagnostics do not call the underlying processes supervisor" do
    {_first, owner, _owner_root, worker_name, gate} = local_pair()
    spec = %{id: :diagnostic, start: {NamedWorker, :start_link, [{worker_name, self(), gate}]}}
    assert {:ok, pid} = Horde.DynamicSupervisor.start_child(owner, spec)
    processes_supervisor = :"#{owner}.ProcessesSupervisor"
    :ok = :sys.suspend(processes_supervisor)

    try do
      assert [{_id, ^pid, %{start: {NamedWorker, :start_link, _}}}] =
               Horde.DynamicSupervisor.local_processes(owner, 1_000)

      assert Horde.DynamicSupervisor.local_process_records(owner, 1_000) ==
               Horde.DynamicSupervisor.local_processes(owner, 1_000)
    after
      :sys.resume(processes_supervisor)
    end
  end

  test "local ownership remains terminable when replication selects another copy" do
    {first, owner, _owner_root, worker_name, gate} = local_pair()
    spec = %{id: :terminable, start: {NamedWorker, :start_link, [{worker_name, self(), gate}]}}
    assert {:ok, original} = Horde.DynamicSupervisor.start_child(owner, spec)
    join_pair(first, owner, original)
    [{id, ^original, child_spec}] = Horde.DynamicSupervisor.local_processes(owner)
    replacement = {{first, node()}, child_spec, gate}
    DeltaCrdt.put(:"#{owner}.Crdt", {:process, id}, replacement)
    await(fn -> Support.snapshot(owner).records == [{id, replacement}] end)
    assert Horde.DynamicSupervisor.local_process_records(owner) == []
    assert [{^id, ^original, _}] = Horde.DynamicSupervisor.local_processes(owner)

    assert :ok = Horde.DynamicSupervisor.terminate_child(owner, original)
    refute Process.alive?(original)
    assert Horde.DynamicSupervisor.local_processes(owner) == []
    assert DeltaCrdt.get(:"#{owner}.Crdt", {:process, id}) == replacement
  end

  test "a new supervisor incarnation on the same connected node clears local suspicion" do
    {first, owner, owner_root, _worker_name, _gate} = local_pair()
    Horde.Cluster.set_members(first, [first, owner])
    await(fn -> map_size(:sys.get_state(first).name_to_supervisor_ref) == 2 end)
    original = Process.whereis(owner)
    Process.exit(owner_root, :kill)
    await(fn -> Map.has_key?(Support.snapshot(first).unavailable, {owner, node()}) end)

    await(fn ->
      Enum.all?([owner, :"#{owner}.Crdt", :"#{owner}.ProcessesSupervisor"], fn name ->
        Process.whereis(name) == nil
      end)
    end)

    start_horde(owner, [first, owner])
    replacement = Process.whereis(owner)
    assert replacement != original

    await(fn ->
      snapshot = Support.snapshot(first)

      snapshot.members[{owner, node()}].pid == replacement and
        not Map.has_key?(snapshot.unavailable, {owner, node()}) and
        Map.has_key?(:sys.get_state(first).name_to_supervisor_ref, {owner, node()})
    end)
  end

  test "an indirect live witness prevents takeover and its disappearance allows recovery" do
    peers = [first, bridge, owner] = start_peers()
    nodes = Enum.map(peers, & &1.node)
    name = "guarded-recovery"

    # Start on the future unreachable owner before joining all three Hordes.
    assert {:ok, original} =
             Cluster.call(owner, Horde.DynamicSupervisor, :start_child, [
               TestSup,
               %{
                 id: :guarded,
                 start: {GuardedWorker, :start_link, [{name, owner.node, bridge.node}]}
               }
             ])

    for peer <- peers, horde <- [TestSup, TestReg] do
      :ok =
        Cluster.call(peer, Horde.Cluster, :set_members, [horde, Enum.map(nodes, &{horde, &1})])
    end

    Cluster.await_supervised(peers, [original])

    await(fn ->
      Enum.all?(peers, fn peer ->
        Cluster.call(peer, Horde.Registry, :lookup, [TestReg, name]) == [{original, nil}]
      end)
    end)

    before =
      Map.new(peers, &{&1.node, Cluster.call(&1, Horde.TestCluster.NodeEvents, :events, [])})

    true = Cluster.call(first, :erlang, :set_cookie, [owner.node, :isolated_first])
    true = Cluster.call(owner, :erlang, :set_cookie, [first.node, :isolated_owner])
    true = Cluster.call(first, Node, :disconnect, [owner.node])

    await(fn ->
      Map.has_key?(
        Cluster.call(first, Support, :snapshot, [TestSup]).unavailable,
        {TestSup, owner.node}
      )
    end)

    Process.sleep(1_100)

    for peer <- peers do
      snapshot = Cluster.call(peer, Support, :snapshot, [TestSup])
      assert Enum.all?(snapshot.members, fn {_, member} -> member.status == :alive end)
      assert [{_, {_, _, ^original}}] = snapshot.records
      assert snapshot.pending == %{}

      events =
        Cluster.call(peer, Horde.TestCluster.NodeEvents, :events, [])
        |> Enum.drop(length(before[peer.node]))

      assert Enum.all?(events, fn {_time, event, other, _info} ->
               event == :nodedown and
                 {peer.node, other} in [{first.node, owner.node}, {owner.node, first.node}]
             end)
    end

    assert Cluster.call(first, Node, :list, []) == [bridge.node]
    assert Enum.sort(Cluster.call(bridge, Node, :list, [])) == Enum.sort([first.node, owner.node])

    # A has already received its only DOWN for C. B's changed observation must
    # release takeover on A without a new local DOWN, a healed link, or an
    # administrative membership removal.
    :ok = Cluster.stop(owner)

    await(fn ->
      case Cluster.call(first, Horde.DynamicSupervisor, :local_processes, [TestSup, 1_000]) do
        [{_, replacement, _}] -> replacement != original and node(replacement) == first.node
        _ -> false
      end
    end)

    assert [{replacement, nil}] = Cluster.call(first, Horde.Registry, :lookup, [TestReg, name])
    assert Cluster.call(first, Process, :alive?, [replacement])
    assert Cluster.call(first, Support, :snapshot, [TestSup]).pending == %{}
  end

  test "the original registry winner retains its logical child record after a successful takeover" do
    peers = [first, owner] = start_peers(~w(a c))
    name = "original-wins"

    assert {:ok, original} =
             Cluster.call(owner, Horde.DynamicSupervisor, :start_child, [
               TestSup,
               %{id: :original, start: {Worker, :start_link, [name]}, restart: :transient}
             ])

    for peer <- peers, horde <- [TestSup, TestReg] do
      :ok =
        Cluster.call(peer, Horde.Cluster, :set_members, [
          horde,
          Enum.map(peers, &{horde, &1.node})
        ])
    end

    Cluster.await_supervised(peers, [original])
    true = Cluster.call(first, :erlang, :set_cookie, [owner.node, :isolated_first])
    true = Cluster.call(owner, :erlang, :set_cookie, [first.node, :isolated_owner])
    true = Cluster.call(first, Node, :disconnect, [owner.node])

    await(fn ->
      match?(
        [{_, pid, _}] when pid != original,
        Cluster.call(first, Horde.DynamicSupervisor, :local_processes, [TestSup])
      )
    end)

    [{logical_id, replacement, _}] =
      Cluster.call(first, Horde.DynamicSupervisor, :local_processes, [TestSup])

    assert [{^logical_id, ^original, _}] =
             Cluster.call(owner, Horde.DynamicSupervisor, :local_processes, [TestSup])

    # Select the original Registry winner before healing, so network delivery
    # cannot legitimately choose and terminate it before this ordering is tested.
    [{^logical_id, replacement_record}] =
      Cluster.call(first, Support, :snapshot, [TestSup]).records

    Cluster.call(first, DeltaCrdt, :put, [
      TestReg.Crdt,
      {:key, name},
      {{TestReg, owner.node}, original, nil}
    ])

    await(fn -> not Cluster.call(first, Process, :alive?, [replacement]) end)
    assert Cluster.call(owner, Process, :alive?, [original])

    for peer <- peers do
      other = if peer == first, do: owner, else: first
      true = Cluster.call(peer, :erlang, :set_cookie, [other.node, Node.get_cookie()])
    end

    await(fn -> Cluster.call(first, Node, :connect, [owner.node]) end)

    await(fn ->
      Enum.all?(peers, &(Cluster.call(&1, Support, :snapshot, [TestSup]).unavailable == %{}))
    end)

    # The supervisor's winning contribution and its owner's cleanup can arrive
    # after Registry conflict resolution. Force that independent ordering: C
    # must retain its live local intent even while both replicas select A's PID.
    Cluster.call(first, DeltaCrdt, :put, [
      TestSup.Crdt,
      {:process, logical_id},
      replacement_record
    ])

    await(fn ->
      Enum.all?(
        peers,
        &(Cluster.call(&1, Support, :snapshot, [TestSup]).records == [
            {logical_id, replacement_record}
          ])
      )
    end)

    Cluster.call(first, Horde.DynamicSupervisorCrdt, :drop_owned, [
      TestSup.Crdt,
      logical_id,
      replacement
    ])

    await(fn ->
      Enum.all?(peers, fn peer ->
        snapshot = Cluster.call(peer, Support, :snapshot, [TestSup])

        match?([{^logical_id, {_, _, ^original}}], snapshot.records) and
          Cluster.call(peer, Horde.Registry, :lookup, [TestReg, name]) == [{original, nil}]
      end)
    end)

    assert Cluster.call(first, Horde.DynamicSupervisor, :local_processes, [TestSup]) == []

    assert [{^logical_id, ^original, _}] =
             Cluster.call(owner, Horde.DynamicSupervisor, :local_processes, [TestSup])
  end

  test "unnamed copies retain local intent without fighting over the healed record" do
    peers = [first, owner] = start_peers(~w(a c))

    assert {:ok, original} =
             Cluster.call(owner, Horde.DynamicSupervisor, :start_child, [
               TestSup,
               {Horde.RecoveryTest.UnnamedWorker, nil}
             ])

    for peer <- peers do
      :ok =
        Cluster.call(peer, Horde.Cluster, :set_members, [
          TestSup,
          Enum.map(peers, &{TestSup, &1.node})
        ])
    end

    Cluster.await_supervised(peers, [original])
    true = Cluster.call(first, :erlang, :set_cookie, [owner.node, :unnamed_cut])
    true = Cluster.call(owner, :erlang, :set_cookie, [first.node, :unnamed_cut_other])
    true = Cluster.call(first, Node, :disconnect, [owner.node])

    await(fn ->
      length(Cluster.call(first, Horde.DynamicSupervisor, :local_processes, [TestSup])) == 1
    end)

    [{id, replacement, _}] =
      Cluster.call(first, Horde.DynamicSupervisor, :local_processes, [TestSup])

    assert replacement != original

    for peer <- peers do
      other = if peer == first, do: owner, else: first
      true = Cluster.call(peer, :erlang, :set_cookie, [other.node, Node.get_cookie()])
    end

    await(fn -> Cluster.call(first, Node, :connect, [owner.node]) end)

    await(fn ->
      snapshots = Enum.map(peers, &Cluster.call(&1, Support, :snapshot, [TestSup]))

      Enum.all?(snapshots, &(&1.unavailable == %{})) and
        length(Enum.uniq(Enum.map(snapshots, & &1.records))) == 1
    end)

    records = Cluster.call(first, Support, :snapshot, [TestSup]).records
    Process.sleep(1_100)
    assert Enum.all?(peers, &(Cluster.call(&1, Support, :snapshot, [TestSup]).records == records))

    assert [{^id, ^original, _}] =
             Cluster.call(owner, Horde.DynamicSupervisor, :local_processes, [TestSup])

    assert [{^id, ^replacement, _}] =
             Cluster.call(first, Horde.DynamicSupervisor, :local_processes, [TestSup])

    assert Cluster.call(owner, Process, :alive?, [original])
    assert Cluster.call(first, Process, :alive?, [replacement])

    # Each local copy remains explicitly terminable, including the copy whose
    # PID is not the CRDT winner. Its cleanup must leave the other copy intact.
    assert :ok =
             Cluster.call(owner, Horde.DynamicSupervisor, :terminate_child, [TestSup, original])

    assert Cluster.call(first, Process, :alive?, [replacement])

    assert :ok =
             Cluster.call(first, Horde.DynamicSupervisor, :terminate_child, [TestSup, replacement])

    await(fn ->
      Enum.all?(peers, &(Cluster.call(&1, Support, :snapshot, [TestSup]).records == []))
    end)
  end

  test "witness paths cross several observers but stale detached views do not prevent failover" do
    peers = [a, b, c, d] = start_peers(~w(a b c d))

    assert {:ok, original} =
             Cluster.call(d, Horde.DynamicSupervisor, :start_child, [
               TestSup,
               %{id: :chain, start: {Worker, :start_link, ["witness-chain"]}, restart: :transient}
             ])

    for peer <- peers, horde <- [TestSup, TestReg] do
      :ok =
        Cluster.call(peer, Horde.Cluster, :set_members, [
          horde,
          Enum.map(peers, &{horde, &1.node})
        ])
    end

    Cluster.await_supervised(peers, [original])

    for {left, right} <- [{a, c}, {a, d}, {b, d}] do
      true = Cluster.call(left, :erlang, :set_cookie, [right.node, :chain_cut])
      true = Cluster.call(right, :erlang, :set_cookie, [left.node, :chain_cut_other])
      true = Cluster.call(left, Node, :disconnect, [right.node])
    end

    await(fn ->
      Enum.all?(
        [{a, [b.node]}, {b, [a.node, c.node]}, {c, [b.node, d.node]}, {d, [c.node]}],
        fn {peer, expected} ->
          Enum.sort(Cluster.call(peer, Node, :list, [])) == Enum.sort(expected)
        end
      )
    end)

    Process.sleep(1_100)
    assert Cluster.call(a, Horde.DynamicSupervisor, :local_processes, [TestSup]) == []
    assert Cluster.call(b, Horde.DynamicSupervisor, :local_processes, [TestSup]) == []
    assert Cluster.call(c, Horde.DynamicSupervisor, :local_processes, [TestSup]) == []

    assert [{logical_id, ^original, _}] =
             Cluster.call(d, Horde.DynamicSupervisor, :local_processes, [TestSup])

    assert Map.has_key?(
             Cluster.call(a, Support, :snapshot, [TestSup]).observations,
             {TestSup, d.node}
           )

    true = Cluster.call(b, :erlang, :set_cookie, [c.node, :chain_cut])
    true = Cluster.call(c, :erlang, :set_cookie, [b.node, :chain_cut_other])
    true = Cluster.call(b, Node, :disconnect, [c.node])

    await(fn ->
      match?(
        [{^logical_id, pid, _}] when pid != original,
        Cluster.call(a, Horde.DynamicSupervisor, :local_processes, [TestSup])
      )
    end)

    assert Cluster.call(d, Process, :alive?, [original])

    assert [{^logical_id, ^original, _}] =
             Cluster.call(d, Horde.DynamicSupervisor, :local_processes, [TestSup])
  end

  defp local_pair do
    suffix = System.unique_integer([:positive])
    first = :"recovery_a_#{suffix}"
    owner = :"recovery_z_#{suffix}"
    worker_name = :"recovery_worker_#{suffix}"
    start_horde(first, [first])
    owner_root = start_horde(owner, [owner])
    gate = start_supervised!({Agent, fn -> :run end})
    {first, owner, owner_root, worker_name, gate}
  end

  defp start_horde(name, members) do
    start_supervised!(%{
      id: make_ref(),
      start:
        {Horde.DynamicSupervisor, :start_link,
         [
           [
             name: name,
             strategy: :one_for_one,
             members: members,
             distribution_strategy: FirstMemberDistribution,
             delta_crdt_options: [sync_interval: 20]
           ]
         ]},
      restart: :temporary
    })
  end

  defp join_pair(first, owner, original) do
    :ok = Horde.Cluster.set_members(first, [first, owner])

    await(fn ->
      Enum.all?([first, owner], fn name ->
        match?([{_, {_, _, ^original}}], Support.snapshot(name).records) and
          map_size(:sys.get_state(name).name_to_supervisor_ref) == 2
      end)
    end)
  end

  defp start_peers(letters \\ ~w(a b c)) do
    for letter <- letters do
      {:ok, controller, node} =
        :peer.start(%{
          name: :"recovery-#{letter}-#{System.unique_integer([:positive])}",
          host: ~c"127.0.0.1",
          longnames: true,
          connection: :standard_io,
          args: [
            ~c"+S",
            ~c"2",
            ~c"-setcookie",
            Atom.to_charlist(Node.get_cookie()),
            ~c"-kernel",
            ~c"prevent_overlapping_partitions",
            ~c"false"
          ]
        })

      peer = %{controller: controller, node: node, manager: nil}
      on_exit(fn -> Cluster.stop(peer) end)
      :ok = Cluster.call(peer, :code, :add_paths, [:code.get_path()])
      {:ok, _} = Cluster.call(peer, Application, :ensure_all_started, [:horde])
      :ok = Cluster.call(peer, Application, :stop, [:test_app])
      {:ok, _} = Cluster.call(peer, Horde.TestCluster.NodeEvents, :start, [])
      :ok = Cluster.call(peer, Support, :start, [])
      peer
    end
    |> then(fn peers ->
      for peer <- peers,
          other <- peers,
          peer != other,
          do: true = Cluster.call(peer, Node, :connect, [other.node])

      peers
    end)
  end

  defp await(check), do: Cluster.await("supervisor recovery", check, 5_000)
end
