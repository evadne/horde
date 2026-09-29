defmodule Horde.ProxyTargetDownTest.LastMemberDistribution do
  @moduledoc false
  def has_quorum?(_members), do: true
  def choose_node(_child_spec, members), do: {:ok, Enum.max_by(members, & &1.name)}
end
