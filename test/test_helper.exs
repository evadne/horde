{_, 0} = System.cmd("epmd", ["-daemon"])

unless Code.ensure_loaded?(:peer) do
  raise "The distributed test suite requires OTP 25 or later for :peer control channels"
end

{:ok, _} = :net_kernel.start([:"manager-#{System.pid()}@127.0.0.1", :longnames])

if Application.get_env(:kernel, :prevent_overlapping_partitions, true) != true do
  raise "Run the test suite with overlapping-partition protection enabled"
end

Application.ensure_all_started(:horde)

ExUnit.start()
