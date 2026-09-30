defmodule Horde.DynamicSupervisorLifecycleTest do
  use ExUnit.Case
  alias Horde.RecoveryTest.{FirstMemberDistribution, UnnamedWorker}
  alias Horde.TestCluster, as: Cluster

  test "incoming recovery waits until the local processes supervisor is ready" do
    name = unique_name()
    start_crdt(name)
    impl = start_impl(name)
    spec = %{id: :incoming, start: {UnnamedWorker, :start_link, [nil]}}
    DeltaCrdt.put(crdt(name), {:process, spec.id}, {nil, spec})

    # This call is a processing barrier, not a delay hiding an early crash.
    assert [{^name, _}] = Horde.Cluster.members(name)
    assert Process.alive?(impl)
    assert DeltaCrdt.get(crdt(name), {:member_node_info, {name, node()}}).status == :uninitialized
    assert Horde.DynamicSupervisor.local_processes(name) == []
    start_processes_supervisor(name)

    await(fn -> match?([{:incoming, _, _}], Horde.DynamicSupervisor.local_processes(name)) end)
    assert Process.whereis(name) == impl
    assert DeltaCrdt.get(crdt(name), {:member_node_info, {name, node()}}).status == :alive
  end

  test "bootstrap materialises existing records and rejects queued stale notifications" do
    name = unique_name()
    start_crdt(name)
    old_pid = spawn(fn -> :ok end)
    ref = Process.monitor(old_pid)
    assert_receive {:DOWN, ^ref, :process, ^old_pid, _}
    spec = %{id: :preexisting, start: {UnnamedWorker, :start_link, [nil]}}
    owned_spec = %{spec | id: :old_incarnation}
    member = {name, node()}
    peer = {:"seeded_peer_#{System.unique_integer([:positive])}", node()}

    DeltaCrdt.merge(crdt(name), %{
      {:member, peer} => 1,
      {:member, member} => 1,
      {:member_node_info, member} => %Horde.DynamicSupervisor.Member{
        name: member,
        status: :alive,
        pid: old_pid
      },
      {:process, spec.id} => {nil, spec},
      {:process, owned_spec.id} => {member, owned_spec, old_pid}
    })

    impl = start_impl(name)

    send(
      impl,
      {:crdt_update,
       [
         {:remove, {:member, member}},
         {:remove, {:process, spec.id}},
         {:add, {:process, owned_spec.id},
          {member, %{owned_spec | start: {MissingBootstrapWorker, :start_link, []}}, old_pid}}
       ]}
    )

    assert Enum.sort(Horde.Cluster.members(name)) == Enum.sort([member, peer])
    start_processes_supervisor(name)
    await(fn -> length(Horde.DynamicSupervisor.local_processes(name)) == 2 end)

    assert Enum.sort(Enum.map(Horde.DynamicSupervisor.local_processes(name), &elem(&1, 0))) == [
             :old_incarnation,
             :preexisting
           ]

    assert Process.whereis(name) == impl
  end

  test "a peer recovers earlier records when an uninitialised supervisor dies on a live node" do
    suffix = System.unique_integer([:positive])
    first = :"early_a_#{suffix}"
    owner = :"early_z_#{suffix}"

    start_supervised!(
      {Horde.DynamicSupervisor,
       name: first, strategy: :one_for_one, distribution_strategy: FirstMemberDistribution}
    )

    start_crdt(owner)
    old_pid = spawn(fn -> :ok end)
    ref = Process.monitor(old_pid)
    assert_receive {:DOWN, ^ref, :process, ^old_pid, _}
    spec = %{id: :earlier_owner, start: {UnnamedWorker, :start_link, [nil]}}

    DeltaCrdt.merge(crdt(owner), %{
      {:member, {first, node()}} => 1,
      {:member, {owner, node()}} => 1,
      {:process, spec.id} => {{owner, node()}, spec, old_pid}
    })

    impl = start_impl(owner)
    :ok = Horde.Cluster.set_members(first, [first, owner])

    await(fn ->
      case :sys.get_state(first).members_info[{owner, node()}] do
        %{status: :uninitialized, pid: ^impl} -> true
        _ -> false
      end
    end)

    assert Horde.DynamicSupervisor.local_processes(first) == []
    Process.exit(impl, :kill)

    await(fn ->
      match?([{:earlier_owner, _, _}], Horde.DynamicSupervisor.local_processes(first))
    end)
  end

  test "shutdown never restarts relinquished children after the processes supervisor stops" do
    name = unique_name()
    root = start_supervised!({Horde.DynamicSupervisor, name: name, strategy: :one_for_one})
    impl = Process.whereis(name)
    assert :ok = GenServer.call(name, :horde_shutting_down)
    assert :ok = Supervisor.terminate_child(root, :"#{name}.ProcessesSupervisor")
    assert Process.whereis(:"#{name}.ProcessesSupervisor") == nil
    member = {name, node()}
    spec = %{id: :late, start: {UnnamedWorker, :start_link, [nil]}}
    # A peer can deliver old :alive information alongside a relinquishment
    # while this supervisor is already draining its children.
    DeltaCrdt.merge(crdt(name), %{
      {:member_node_info, member} => %Horde.DynamicSupervisor.Member{
        name: member,
        status: :alive,
        pid: impl
      },
      {:process, spec.id} => {nil, spec}
    })

    send(
      impl,
      {:crdt_update,
       [
         {:add, {:member_node_info, member},
          %Horde.DynamicSupervisor.Member{name: member, status: :alive, pid: impl}},
         {:add, {:process, spec.id}, {nil, spec}}
       ]}
    )

    assert [^member] = Horde.Cluster.members(name)
    assert Process.alive?(impl)
    assert Horde.DynamicSupervisor.local_processes(name) == []
    assert DeltaCrdt.get(crdt(name), {:process, spec.id}) == {nil, spec}

    await(fn ->
      DeltaCrdt.get(crdt(name), {:member_node_info, member}).status == :shutting_down
    end)
  end

  defp start_crdt(name),
    do:
      start_supervised!(
        {DeltaCrdt,
         crdt: Horde.DynamicSupervisorCrdt,
         name: crdt(name),
         on_diffs: {Horde.DynamicSupervisorImpl, :on_diffs, [name]}}
      )

  defp start_impl(name) do
    start_supervised!(%{
      id: name,
      restart: :temporary,
      start:
        {Horde.DynamicSupervisorImpl, :start_link,
         [
           [
             name: name,
             members: [name],
             strategy: :one_for_one,
             distribution_strategy: FirstMemberDistribution,
             process_redistribution: :passive
           ]
         ]}
    })
  end

  defp start_processes_supervisor(name),
    do:
      start_supervised!(
        {Horde.ProcessesSupervisor,
         root_name: name, name: :"#{name}.ProcessesSupervisor", strategy: :one_for_one}
      )

  defp unique_name, do: :"lifecycle_#{System.unique_integer([:positive])}"
  defp crdt(name), do: :"#{name}.Crdt"
  defp await(check), do: Cluster.await("supervisor lifecycle convergence", check)
end
