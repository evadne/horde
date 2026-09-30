defmodule Horde.DynamicSupervisorCrdt do
  @moduledoc false
  alias DeltaCrdt.AWLWWMap

  # Preserve AWLWWMap's wire format while making local lifecycle operations
  # conditional on the PID that actually owned the child.
  defdelegate new(), to: AWLWWMap
  defdelegate read(state), to: AWLWWMap
  defdelegate read(state, keys), to: AWLWWMap
  defdelegate compress_dots(state), to: AWLWWMap
  defdelegate join(left, right, keys), to: AWLWWMap
  defdelegate add(key, value, actor, state), to: AWLWWMap
  defdelegate remove(key, actor, state), to: AWLWWMap

  def drop_owned(crdt, id, pid), do: operation(crdt, :remove_owner, [{:process, id}, pid])
  def drop_relinquished(crdt, id), do: operation(crdt, :remove_relinquished, [{:process, id}])

  def relinquish_owned(crdt, id, pid, spec),
    do: operation(crdt, :relinquish_owner, [{:process, id}, pid, spec])

  def restore_owned(crdt, id, record),
    do: operation(crdt, :restore_owner, [{:process, id}, record])

  defp operation(crdt, operation, args),
    do: GenServer.call(crdt, {:operation, {operation, args}}, :infinity)

  def remove_owner(key, pid, _actor, state) do
    dots =
      Enum.flat_map(Map.get(state.value, key, %{}), fn
        {{{_member, _spec, ^pid}, _timestamp}, dots} -> Enum.to_list(dots)
        _ -> []
      end)

    %AWLWWMap{dots: MapSet.new(dots)}
  end

  def remove_relinquished(key, _actor, state) do
    dots =
      Enum.flat_map(Map.get(state.value, key, %{}), fn
        {{{nil, _spec}, _timestamp}, dots} -> Enum.to_list(dots)
        _ -> []
      end)

    %AWLWWMap{dots: MapSet.new(dots)}
  end

  def relinquish_owner(key, pid, spec, actor, state) do
    values = Map.keys(Map.get(state.value, key, %{}))

    if values != [] and Enum.all?(values, &match?({{_, _, ^pid}, _}, &1)) do
      AWLWWMap.add(key, {nil, spec}, actor, state)
    else
      # A replacement already exists. Relinquishing the previous copy must not
      # replace its record with another request to start the same child.
      remove_owner(key, pid, actor, state)
    end
  end

  def restore_owner(key, record, actor, state) do
    case Map.get(AWLWWMap.read(state, [key]), key) do
      nil -> AWLWWMap.add(key, record, actor, state)
      {nil, _spec} -> AWLWWMap.add(key, record, actor, state)
      _other_owner -> AWLWWMap.new()
    end
  end
end
