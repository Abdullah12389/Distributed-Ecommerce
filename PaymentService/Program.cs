using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;

double balance = 5177.67;
_ = Task.Run(async () =>
{
    var ccfg = new ConsumerConfig{
        BootstrapServers="localhost:9092",
        GroupId = $"fresh-consumer-group-{Guid.NewGuid()}",
        AutoOffsetReset = AutoOffsetReset.Earliest
    };
    var consumer = new ConsumerBuilder<Null, string>(ccfg).Build();
    consumer.Subscribe(new[] {"payment_initiation","price_result"});
    while (true)
    {
        try
        {
            var result = consumer.Consume(CancellationToken.None);
            if(result.Topic == "payment_initiation")
            {
                Console.WriteLine("Initializing Payment");
                if(result?.Message != null)
                {
                    var order = JsonSerializer.Deserialize<Order>(result.Message.Value);
                    var pConfig = new ProducerConfig{BootstrapServers = "localhost:9092"};
                    var producer = new ProducerBuilder<Null, string>(pConfig).Build();
                    await producer.ProduceAsync("price_confirmation", new Message<Null, string>{
                        Value = JsonSerializer.Serialize(order)
                    }); 
                    Console.WriteLine("Asking About The Amount from Inventory");
                }
            }
            else if(result.Topic == "price_result")
            {
                if(result?.Message != null)
                {
                    var priceResult = JsonSerializer.Deserialize<Product_Price>(result.Message.Value);
                    Console.WriteLine($"Total Amount {priceResult.Total}$ Paying...");
                    balance -= priceResult.Total;
                    var pConfig = new ProducerConfig{BootstrapServers = "localhost:9092"};
                    var producer = new ProducerBuilder<Null, string>(pConfig).Build();
                    await producer.ProduceAsync("payment_status", new Message<Null, string>{Value = JsonSerializer.Serialize(priceResult)});
                    Console.WriteLine("Payment Done...Telling Others about it");  
                }
            }
        }
        catch(Exception ex)
        {
            Console.WriteLine($"Exception Occured {ex}");
        }
        // finally
        // {
        //     Console.WriteLine($"Balance left is : {balance}$");
        //     consumer.Close();
        // }  
    }
});
Thread.Sleep(Timeout.Infinite);
public struct Order
{
    public int Id{get;}
    public int Pid{get;set;}
    public int Quantity{get;set;}
}
public struct Product_Price
{
    public int Oid{get;set;}
    public int Pid{get;set;}
    public int Quantity{get;set;}
    public double Total{get;set;}
}