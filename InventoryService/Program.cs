using System.Text.Json;
using Confluent.Kafka;

List<Product> products = new List<Product>{};
bool isRunning = true;
_ = Task.Run(()=>{
    while (isRunning)
    {
        Console.WriteLine("Press (1) for Add\nPress (2) to see All\nPress (q) to exit:\n");
        char.TryParse(Console.ReadLine(),out char choice);
        switch (choice)
        {
            case '1':
                Console.Write("Enter the Product Name: ");
                string? name = Console.ReadLine();
                Console.Write("Enter the Stock: ");
                int.TryParse(Console.ReadLine(),out int stock);
                Console.Write("Enter the Price/Piece: ");
                double.TryParse(Console.ReadLine(),out double price);
                Product Product = new Product
                {
                    Name = name ?? "",
                    Stock = stock,
                    Price = price
                };
                products.Add(Product);
                break;
            case '2':
                foreach(var ord in products)
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
_  = Task.Run(async () =>
{
    var ccfg = new ConsumerConfig{
        BootstrapServers = "localhost:9092",
        GroupId = $"fresh-consumer-group-{Guid.NewGuid()}",
        AutoOffsetReset = AutoOffsetReset.Earliest
    };
    using var consumer = new ConsumerBuilder<Null, string>(ccfg).Build();
    consumer.Subscribe(new[] {"order_created","price_confirmation","payment_status"});
    while (true)
    {
        try
        {
            var result = consumer.Consume(CancellationToken.None);
            if(result.Topic == "order_created")
            {
                if(result?.Message != null)
                {
                    Console.WriteLine("Message Received Now I will check is order valid");
                    var order = JsonSerializer.Deserialize<Order>(result.Message.Value);
                    bool isExist = products.Any(p=> p.Pid == order.Pid && p.Stock >= order.Quantity);
                    var pConfig = new ProducerConfig{BootstrapServers = "localhost:9092"};
                    var producer = new ProducerBuilder<Null, string>(pConfig).Build();
                    var productExist = new ProductExist{
                                    Oid =  order.Id,
                                    isExist = isExist
                                };
                    await producer.ProduceAsync("order_verified", new Message<Null, string>{
                        Value = JsonSerializer.Serialize(productExist)
                    });        
                    Console.WriteLine("Told that the product Exist");             
                }   
            }
            else if(result.Topic == "price_confirmation")
            {
                if(result?.Message != null)
                {
                    Console.WriteLine("Payment Asked About Price of order");
                    var order = JsonSerializer.Deserialize<Order>(result.Message.Value);
                    Product? product = products.FirstOrDefault(p=> p.Pid == order.Pid);
                    var pConfig = new ProducerConfig{BootstrapServers = "localhost:9092"};
                    var producer = new ProducerBuilder<Null, string>(pConfig).Build();
                    if(product!= null)
                    {
                        var pp =  new Product_Price{
                                    Pid = order.Pid,
                                    Oid =  order.Id,
                                    Quantity = order.Quantity,
                                    Total = product.Price*order.Quantity
                                };
                        await producer.ProduceAsync("price_result", new Message<Null, string>{
                            Value = JsonSerializer.Serialize(pp)
                        }); 
                        Console.WriteLine($"Told price {product.Price*order.Quantity}");
                    }

                }   
            }
            else if(result.Topic == "payment_status")
            {
                if(result?.Message != null)
                {
                    Console.WriteLine("Payment Successful Reducing Stock");
                    var product = JsonSerializer.Deserialize<Product_Price>(result.Message.Value);
                    var existingProduct = products.FirstOrDefault(p=>p.Pid == product.Pid); 
                    if(existingProduct != null)
                    {
                        existingProduct.Stock -= product.Quantity;
                    }
                    Console.WriteLine($"Stock Left for {existingProduct?.Name} is {existingProduct?.Stock} pieces");
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
public struct Order
{
    public int Id{get;set;}
    public int Pid{get;set;}
    public int Quantity{get;set;}
}
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