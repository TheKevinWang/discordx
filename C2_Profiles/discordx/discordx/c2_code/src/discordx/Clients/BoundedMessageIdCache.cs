namespace discordx.Clients
{
    internal sealed class BoundedMessageIdCache
    {
        private readonly int _capacity;
        private readonly Queue<ulong> _order = new();
        private readonly HashSet<ulong> _ids = new();
        private readonly object _gate = new();

        public BoundedMessageIdCache(int capacity)
        {
            if (capacity <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity));
            }
            _capacity = capacity;
        }

        public bool Contains(ulong messageId)
        {
            lock (_gate)
            {
                return _ids.Contains(messageId);
            }
        }

        public bool TryAdd(ulong messageId)
        {
            lock (_gate)
            {
                if (!_ids.Add(messageId))
                {
                    return false;
                }
                _order.Enqueue(messageId);
                while (_order.Count > _capacity)
                {
                    _ids.Remove(_order.Dequeue());
                }
                return true;
            }
        }

        public void Add(ulong messageId)
        {
            _ = TryAdd(messageId);
        }

        public void Remove(ulong messageId)
        {
            lock (_gate)
            {
                if (!_ids.Remove(messageId))
                {
                    return;
                }

                if (_order.Count == 0)
                {
                    return;
                }

                var retained = _order.Where(id => id != messageId).ToArray();
                _order.Clear();
                foreach (var id in retained)
                {
                    _order.Enqueue(id);
                }
            }
        }
    }
}
