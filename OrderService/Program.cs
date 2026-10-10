using System.Text.Json;
using Confluent.Kafka;
List<Order> orders = new List<Order>{};

_ = Task.Run(async ()=>{
    bool isRunning = true;
    while (isRunning)
    {
        Console.WriteLine("Press (1) for Add\nPress (2) to see All\nPress (q) to exit:\n");
        char.TryParse(Console.ReadLine(),out char choice);
        switch (choice)
        {
            case '1':
                Console.Write("Enter the Product ID: ");
                int.TryParse(Console.ReadLine(),out int pid);
                Console.Write("Enter the Quantity: ");
                int.TryParse(Console.ReadLine(),out int qty);
                Order order = new Order
                {
                    Pid = pid,
                    Quantity = qty
                };
                orders.Add(order);
                var pConfig = new ProducerConfig{BootstrapServers = "localhost:9092"};
                string jsonOrder = JsonSerializer.Serialize(order);
                var producer = new ProducerBuilder<Null, string>(pConfig).Build();
                await producer.ProduceAsync("order_created", new Message<Null, string>{ Value = jsonOrder });
                Console.WriteLine("Asked does product exist");
                break;
            case '2':
                foreach(var ord in orders)
                {
                    Console.WriteLine(ord.ToString());
                }
                break;
            case 'q':
                isRunning = false;
                break;
        }
    }
});
_ = Task.Run(async () =>
{
    var ccfg = new ConsumerConfig{
        BootstrapServers="localhost:9092",
        GroupId = $"fresh-consumer-group-{Guid.NewGuid()}",
        AutoOffsetReset = AutoOffsetReset.Earliest
    };
    var consumer = new ConsumerBuilder<Null, string>(ccfg).Build();
    consumer.Subscribe(new[] {"order_verified","payment_status"});
    while (true)
    {
        try
        {
            var result = consumer.Consume(CancellationToken.None);
            if(result.Topic == "order_verified")
            {
                if(result?.Message != null)
                {
                    var order = JsonSerializer.Deserialize<ProductExist>(result.Message.Value);
                    Console.WriteLine($"This is order received {order.Oid}");
                    var ord = orders.FirstOrDefault(o=> o.Id == order.Oid);
                    Console.WriteLine($"The product {(order.isExist?"Exist":"NotExist")} Receieved");
                    if(ord!= null)
                    {
                        Console.WriteLine($"Order found {ord.Id}");
                        var pConfig = new ProducerConfig{BootstrapServers = "localhost:9092"};
                        var producer = new ProducerBuilder<Null, string>(pConfig).Build();
                        await producer.ProduceAsync("payment_initiation", new Message<Null, string>
                        {
                            Value = JsonSerializer.Serialize(ord)
                        }); 
                        Console.WriteLine("Now asking for the payment initiation");  
                    }

                }
            }
            else if(result.Topic == "payment_status")
            {
                if(result?.Message != null)
                {
                    Console.WriteLine("Payment is Done Order Completed Setting new Status");
                    var priceObj = JsonSerializer.Deserialize<Product_Price>(result.Message.Value);
                    var order = orders.FirstOrDefault(o=>o.Id == priceObj.Oid); 
                    if(order != null)
                    {
                        order.Status = "Paid";
                    }
                    Console.WriteLine(order?.ToString());
                }
            }
        }
        catch(Exception ex)
        {
            Console.WriteLine($"Exception Occurred {ex}");
        }
        // finally
        // {
        //     consumer.Close();
        // }   
    }

});
Thread.Sleep(Timeout.Infinite);
public struct ProductExist
{
    public int Oid{get;set;}
    public bool isExist{get;set;}
}
public struct Product_Price
{
    public int Oid{get;set;}
    public int Pid{get;set;}
    public int Quantity{get;set;}
    public double Total{get;set;}
}