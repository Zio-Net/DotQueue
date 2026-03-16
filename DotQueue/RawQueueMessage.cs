namespace DotQueue;

/// <summary>
/// Raw queue payload for typed routing scenarios where concrete message type
/// is selected from metadata (for example, "messageType") before deserialization.
/// </summary>
public sealed record RawQueueMessage(string Body);
