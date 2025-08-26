namespace DotQueue;

public class RetryableException : Exception
{
    public RetryableException(string message) : base(message) { }
    public RetryableException(string message, Exception inner) : base(message, inner) { }
}

public class NonRetryableException : Exception
{
    public NonRetryableException(string message) : base(message) { }
    public NonRetryableException(string message, Exception inner) : base(message, inner) { }
}
