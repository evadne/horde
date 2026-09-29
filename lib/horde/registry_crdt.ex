defmodule Horde.RegistryCrdt do
  @moduledoc false
  alias DeltaCrdt.AWLWWMap

  # Keep AWLWWMap's state and wire format. Only local cleanup gains an operation
  # that removes the observed contributions of one owner, rather than every
  # value for a key. Older peers can still merge these ordinary AWLWWMap deltas.
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
