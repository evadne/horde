defmodule Horde.RegistryCrdt do
  @moduledoc false
  alias DeltaCrdt.AWLWWMap

  # Keep AWLWWMap's state and wire format. Local operations guard owner and member
  # lifecycles atomically; older peers can still merge these ordinary AWLWWMap
  # deltas, but do not provide the same lifecycle guarantees themselves.
  defdelegate new(), to: AWLWWMap
  defdelegate read(state), to: AWLWWMap
  defdelegate read(state, keys), to: AWLWWMap
  defdelegate compress_dots(state), to: AWLWWMap
  defdelegate join(left, right, keys), to: AWLWWMap
  defdelegate add(key, value, actor, state), to: AWLWWMap
  defdelegate remove(key, actor, state), to: AWLWWMap

  def drop_owned(crdt, keys, pid) do
    # DeltaCrdt 0.6's custom-operation protocol executes this comparison and
    # removal in the CRDT process. A separate get followed by delete would race
    # with incoming replication just as the registry's delayed ETS view does.
    operations = Enum.map(keys, &{:remove_owner, [{:key, &1}, pid]})
    GenServer.call(crdt, {:bulk_operation, operations}, :infinity)
  end

  def put_registration(crdt, key, registration) do
    GenServer.call(crdt, {:operation, {:register_member, [{:key, key}, registration]}}, :infinity)
  end

  def restore_registration(crdt, key, registration) do
    GenServer.call(crdt, {:operation, {:restore_member, [{:key, key}, registration]}}, :infinity)
  end

  def drop_member(crdt, member, keys) do
    operations = Enum.map(keys, &{:remove_member_registration, [{:key, &1}, member]})
    GenServer.call(crdt, {:bulk_operation, operations}, :infinity)
  end

  def register_member(key, {member, _pid, _value} = registration, actor, state) do
    if Map.has_key?(state.value, {:member, member}) do
      AWLWWMap.add(key, registration, actor, state)
    else
      AWLWWMap.new()
    end
  end

  def restore_member(key, registration, actor, state) do
    if Map.has_key?(state.value, key) do
      AWLWWMap.new()
    else
      register_member(key, registration, actor, state)
    end
  end

  def remove_member_registration(key, member, _actor, state) do
    # A queued removal notification must not delete claims after the member has
    # already rejoined, nor remove a replacement owner's concurrent contribution.
    dots =
      if Map.has_key?(state.value, {:member, member}) do
        []
      else
        Enum.flat_map(Map.get(state.value, key, %{}), fn
          {{{^member, _pid, _value}, _timestamp}, dots} -> Enum.to_list(dots)
          _ -> []
        end)
      end

    %AWLWWMap{dots: MapSet.new(dots)}
  end

  def remove_owner(key, pid, _actor, state) do
    dots =
      state.value
      |> Map.get(key, %{})
      |> Enum.flat_map(fn
        {{{_member, ^pid, _value}, _timestamp}, dots} -> Enum.to_list(dots)
        _ -> []
      end)

    %AWLWWMap{dots: MapSet.new(dots)}
  end
end
