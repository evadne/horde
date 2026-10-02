defmodule Horde.RegistryRemoteConflictTest do
  use ExUnit.Case
  alias Horde.TestCluster, as: Cluster

  setup do
    peers = [first, owner] = Cluster.start_nodes("registry-conflict", 2, connect_manager: false)

    for peer <- peers do
      Cluster.call(peer, Code, :compile_string, [
        ~S"""
        defmodule Horde.RegistryConflictWitness do
          use GenServer
          def start(pid), do: GenServer.start(__MODULE__, pid)
          def init(pid), do: {:ok, %{ref: Process.monitor(pid), result: :alive}}
          def handle_info({:DOWN, ref, :process, _pid, reason}, %{ref: ref} = state),
            do: {:noreply, %{state | result: {:down, reason}}}
          def handle_call(:result, _, state), do: {:reply, state.result, state}
          def candidate, do: Agent.start(fn -> nil end)
        end
        """
      ])
    end

    assert {:ok, original} = Cluster.call(owner, Worker, :start, ["remote-conflict"])
    assert {:ok, witness} = Cluster.call(owner, Horde.RegistryConflictWitness, :start, [original])
    assert {:ok, candidate} = Cluster.call(first, Horde.RegistryConflictWitness, :candidate, [])

    for peer <- peers, name <- [TestSup, TestReg] do
      :ok =
        Cluster.call(peer, Horde.Cluster, :set_members, [name, Enum.map(peers, &{name, &1.node})])
    end

    Cluster.heal(peers)

    Cluster.await("original registration on both peers", fn ->
      Enum.all?(
        peers,
        &(Cluster.call(&1, Horde.Registry, :lookup, [TestReg, "remote-conflict"]) == [
            {original, nil}
          ])
      )
    end)

    :ok = Cluster.call(owner, :sys, :suspend, [TestReg])

    on_exit(fn ->
      try do
        IO.inspect(Cluster.call(owner, GenServer, :call, [witness, :result]),
          label: "ORIGINAL EXIT WITNESS"
        )

        Cluster.call(owner, :sys, :resume, [TestReg])
      catch
        kind, reason -> IO.inspect({kind, reason}, label: "WITNESS CLEANUP")
      end
    end)

    %{first: first, owner: owner, original: original, witness: witness, candidate: candidate}
  end

  test "a superseded foreign winner cannot retire the surviving original", context do
    %{first: first, owner: owner, original: original, witness: witness, candidate: candidate} =
      context

    publish_candidate(context)

    # The owner's Registry is paused while its CRDT continues receiving deltas.
    # Replace the temporary foreign winner before the owner reconciles. A remote
    # Process.exit sent by the foreign Registry cannot be revoked by this update.
    Cluster.call(first, DeltaCrdt, :put, [
      TestReg.Crdt,
      {:key, "remote-conflict"},
      {{TestReg, owner.node}, original, nil}
    ])

    Cluster.await("restored original reaches the owner's CRDT", fn ->
      Cluster.call(owner, DeltaCrdt, :get, [TestReg.Crdt, {:key, "remote-conflict"}]) ==
        {{TestReg, owner.node}, original, nil}
    end)

    :ok = Cluster.call(owner, :sys, :resume, [TestReg])

    Cluster.await("owner has processed queued registration diffs", fn ->
      Cluster.call(owner, Horde.Registry, :lookup, [TestReg, "remote-conflict"]) == [
        {original, nil}
      ]
    end)

    assert Cluster.call(owner, GenServer, :call, [witness, :result]) == :alive
    assert Cluster.call(owner, Process, :alive?, [original])

    Cluster.await("candidate retires at its own Registry", fn ->
      not Cluster.call(first, Process, :alive?, [candidate])
    end)
  end

  test "a surviving foreign winner causes the owner's Registry to retire its loser", context do
    %{first: first, owner: owner, original: original, witness: witness, candidate: candidate} =
      context

    publish_candidate(context)
    assert Cluster.call(owner, Process, :alive?, [original])
    assert Cluster.call(owner, GenServer, :call, [witness, :result]) == :alive
    :ok = Cluster.call(owner, :sys, :resume, [TestReg])

    Cluster.await("owner retires the local losing transient worker normally", fn ->
      Cluster.call(owner, GenServer, :call, [witness, :result]) == {:down, :normal}
    end)

    assert Cluster.call(first, Process, :alive?, [candidate])

    assert Cluster.call(owner, Horde.Registry, :lookup, [TestReg, "remote-conflict"]) == [
             {candidate, nil}
           ]
  end

  defp publish_candidate(%{first: first, owner: owner, candidate: candidate}) do
    Cluster.call(first, DeltaCrdt, :put, [
      TestReg.Crdt,
      {:key, "remote-conflict"},
      {{TestReg, first.node}, candidate, nil}
    ])

    Cluster.await("candidate is visible locally and replicated to paused owner", fn ->
      Cluster.call(first, Horde.Registry, :lookup, [TestReg, "remote-conflict"]) == [
        {candidate, nil}
      ] and
        Cluster.call(owner, DeltaCrdt, :get, [TestReg.Crdt, {:key, "remote-conflict"}]) ==
          {{TestReg, first.node}, candidate, nil}
    end)
  end
end
