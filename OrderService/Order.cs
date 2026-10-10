class Order
{
    private static int _count=0;
    public Order()
    {
        Id = Interlocked.Increment(ref _count);
    }
    public int Id{get;}
    public int Pid{get;set;}
    public int Quantity{get;set;}
    public string Status{get;set;} = "Unpaid";
    public override string ToString()
    {
        return $"ID: {Pid}, OrderId: {Id}, Quantity: {Quantity}, Status: {Status}";
    }
}