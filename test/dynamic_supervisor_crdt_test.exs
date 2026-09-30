defmodule Horde.DynamicSupervisorCrdtTest do
  use ExUnit.Case, async: true
  alias Horde.DynamicSupervisorCrdt, as: Crdt
  alias DeltaCrdt.AWLWWMap

  test "a previous owner cannot delete or relinquish a replacement's record" do
    previous =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> Process.exit(previous, :kill) end)
    key = {:process, :child}
    spec = %{id: :child, start: {Agent, :start_link, [fn -> :ok end]}}
    replacement = {{:supervisor, node()}, spec, self()}
    state = AWLWWMap.add(key, replacement, :actor, AWLWWMap.new())

    for delta <- [
          Crdt.remove_owner(key, previous, :actor, state),
          Crdt.relinquish_owner(key, previous, spec, :actor, state)
        ] do
      assert AWLWWMap.read(AWLWWMap.join(state, delta, [key]))[key] == replacement
    end
  end

  test "a late relinquishment cannot start another copy of a restored live owner" do
    key = {:process, :child}
    spec = %{id: :child}
    state = AWLWWMap.add(key, {nil, spec}, :previous, AWLWWMap.new())
    record = {{:supervisor, node()}, spec, self()}
    restored = AWLWWMap.join(state, Crdt.restore_owner(key, record, :owner, state), [key])
    assert AWLWWMap.read(restored)[key] == record
  end
end
