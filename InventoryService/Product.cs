class Product
{
    private static int _count=0;
    public Product()
    {
        Pid = Interlocked.Increment(ref _count);
    }
    public int Pid {get;}
    public string Name{get;set;} = string.Empty;
    public double Price{get;set;}
    public int Stock{get;set;}
    public override string ToString()
    {
        return $"ID: {Pid}, Name: {Name}, Stock: {Stock}, Status: {Price}$";
    }

}