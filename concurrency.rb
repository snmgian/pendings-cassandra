require 'cassandra'


column_name = 'value'


threads = []
4.times do
  threads << Thread.new do
    client = Cassandra.new('Concurrency')
    client.remove(:Case, 'subject')
    client.insert(:Case, 'subject', {column_name => '0'})
    10.times do
      begin
        puts '.'
        sleep 0.3
        value = client.get(:Case, 'subject')[column_name].to_i
        value += 1
        client.insert(:Case, 'subject', {column_name => value.to_s})
      rescue
        puts $!
      end
    end
  end
end

threads.each { |t| t.join }

client = Cassandra.new('Concurrency')
value = client.get(:Case, 'subject')[column_name]
puts "Final value: #{value}"
