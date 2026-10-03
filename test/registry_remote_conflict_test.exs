defmodule Horde.RegistryRemoteConflictTest do
  use ExUnit.Case

  @peer_support ~S"""
  defmodule Horde.RegistryConflictSubject do
    use GenServer

    def start(registry, key), do: GenServer.start(__MODULE__, {registry, key})

    def init({registry, key}) do
      Process.flag(:trap_exit, true)
      {:ok, _pid} = Horde.Registry.register(registry, key, nil)
      {:ok, nil}
    end

    def handle_info({:EXIT, _from, {:name_conflict, _, _, _}}, state) do
      {:stop, :normal, state}
    end

    def candidate, do: Agent.start(fn -> nil end)
  end

  defmodule Horde.RegistryConflictWitness do
    use GenServer

    def start(pid), do: GenServer.start(__MODULE__, pid)
    def init(pid), do: {:ok, %{ref: Process.monitor(pid), result: :alive}}

    def handle_info({:DOWN, ref, :process, _pid, reason}, %{ref: ref} = state) do
      {:noreply, %{state | result: {:down, reason}}}
    end

    def handle_call(:result, _from, state), do: {:reply, state.result, state}
  end
  """

  setup do
    peers = [first, owner] = start_nodes(2)

    for peer <- peers do
      call(peer, Code, :compile_string, [@peer_support])
    end

    assert {:ok, original} =
             call(owner, Horde.RegistryConflictSubject, :start, [TestReg, "remote-conflict"])

    assert {:ok, witness} =
             call(owner, Horde.RegistryConflictWitness, :start, [original])

    assert {:ok, candidate} = call(first, Horde.RegistryConflictSubject, :candidate, [])

    members = Enum.map(peers, &{TestReg, &1.node})

    for peer <- peers do
      :ok = call(peer, Horde.Cluster, :set_members, [TestReg, members])
    end

    connect(peers)

    await("original registration on both peers", fn ->
      Enum.all?(peers, fn peer ->
        call(peer, Horde.Registry, :lookup, [TestReg, "remote-conflict"]) == [{original, nil}]
      end)
    end)

    :ok = call(owner, :sys, :suspend, [TestReg])

    on_exit(fn ->
      try do
        call(owner, :sys, :resume, [TestReg])
      catch
        :exit, _reason -> :ok
      end
    end)

    %{first: first, owner: owner, original: original, witness: witness, candidate: candidate}
  end

  test "a superseded foreign winner cannot retire the surviving original", context do
    %{first: first, owner: owner, original: original, witness: witness, candidate: candidate} =
      context

    publish_candidate(context)

    # Restore the original while the owner's Registry is paused. A remote exit
    # sent for the temporary foreign winner cannot be revoked by this update.
    call(first, DeltaCrdt, :put, [
      TestReg.Crdt,
      {:key, "remote-conflict"},
      {{TestReg, owner.node}, original, nil}
    ])

    await("restored original reaches the owner's CRDT", fn ->
      call(owner, DeltaCrdt, :get, [TestReg.Crdt, {:key, "remote-conflict"}]) ==
        {{TestReg, owner.node}, original, nil}
    end)

    :ok = call(owner, :sys, :resume, [TestReg])

    await("owner has processed queued registration diffs", fn ->
      call(owner, Horde.Registry, :lookup, [TestReg, "remote-conflict"]) == [{original, nil}]
    end)

    assert call(owner, GenServer, :call, [witness, :result]) == :alive
    assert call(owner, Process, :alive?, [original])

    await("candidate retires at its own Registry", fn ->
      not call(first, Process, :alive?, [candidate])
    end)
  end

  test "a surviving foreign winner causes the owner's Registry to retire its loser", context do
    %{first: first, owner: owner, original: original, witness: witness, candidate: candidate} =
      context

    publish_candidate(context)
    assert call(owner, Process, :alive?, [original])
    assert call(owner, GenServer, :call, [witness, :result]) == :alive
    :ok = call(owner, :sys, :resume, [TestReg])

    await("owner retires the local losing process normally", fn ->
      call(owner, GenServer, :call, [witness, :result]) == {:down, :normal}
    end)

    assert call(first, Process, :alive?, [candidate])

    assert call(owner, Horde.Registry, :lookup, [TestReg, "remote-conflict"]) == [
             {candidate, nil}
           ]
  end

  defp publish_candidate(%{first: first, owner: owner, candidate: candidate}) do
    call(first, DeltaCrdt, :put, [
      TestReg.Crdt,
      {:key, "remote-conflict"},
      {{TestReg, first.node}, candidate, nil}
    ])

    await("candidate is visible locally and replicated to paused owner", fn ->
      call(first, Horde.Registry, :lookup, [TestReg, "remote-conflict"]) == [
        {candidate, nil}
      ] and
        call(owner, DeltaCrdt, :get, [TestReg.Crdt, {:key, "remote-conflict"}]) ==
          {{TestReg, first.node}, candidate, nil}
    end)
  end

  defp start_nodes(count) do
    for index <- 1..count do
      name = :"registry-conflict-#{System.unique_integer([:positive])}-#{index}"

      {:ok, controller, node} =
        :peer.start(%{
          name: name,
          host: ~c"127.0.0.1",
          longnames: true,
          connection: :standard_io,
          args: [~c"+S", ~c"2", ~c"-setcookie", Atom.to_charlist(Node.get_cookie())]
        })

      peer = %{controller: controller, node: node}
      on_exit(fn -> stop(peer) end)
      :ok = call(peer, :code, :add_paths, [:code.get_path()])
      {:ok, _} = call(peer, Application, :ensure_all_started, [:horde])
      {:ok, _} = call(peer, Application, :ensure_all_started, [:test_app])
      peer
    end
  end

  defp connect(peers) do
    nodes = Enum.map(peers, & &1.node)

    for peer <- peers, other <- nodes, other != peer.node do
      true = call(peer, Node, :connect, [other])
    end

    for peer <- peers do
      :ok = call(peer, :global, :sync, [])
    end

    await("connected peer pair", fn ->
      Enum.all?(peers, fn peer ->
        Enum.sort(call(peer, Node, :list, [])) == Enum.sort(nodes -- [peer.node])
      end)
    end)
  end

  defp call(peer, module, function, arguments) do
    :peer.call(peer.controller, module, function, arguments, 5_000)
  end

  defp stop(peer) do
    try do
      :peer.stop(peer.controller)
    catch
      :exit, :noproc -> :ok
      :exit, {:noproc, _} -> :ok
    end
  end

  defp await(description, check, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_until(description, check, deadline)
  end

  defp await_until(description, check, deadline) do
    unless check.() do
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("Timed out waiting for #{description}")
      end

      Process.sleep(10)
      await_until(description, check, deadline)
    end
  end
end
