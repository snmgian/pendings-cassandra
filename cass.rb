require 'cassandra'

client = Cassandra.new('Twissandra')

client.insert(:User, 'paez', {'age' => '42', 'first' => 'Fito', 'last' => 'Paez'})
puts client.get(:User, 'paez')

#client.remove(:User, 'paez')
