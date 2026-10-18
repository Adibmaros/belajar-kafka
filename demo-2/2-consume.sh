B=/opt/kafka/bin
S=kafka:29092
T=120

echo "== Menjalankan consumer group selama $T detik =="

# order-service: baca semua, lag 0
timeout $T $B/kafka-console-consumer.sh --bootstrap-server $S --topic orders --group order-service --from-beginning > /dev/null 2>&1 &

# payment-service: hanya 1000 message, lalu berhenti (ada lag)
timeout $T $B/kafka-console-consumer.sh --bootstrap-server $S --topic payments --group payment-service --from-beginning --max-messages 1000 > /dev/null 2>&1 &

# user-service: 2 consumer dalam 1 group, partisi terbagi
timeout $T $B/kafka-console-consumer.sh --bootstrap-server $S --topic users --group user-service --from-beginning > /dev/null 2>&1 &
timeout $T $B/kafka-console-consumer.sh --bootstrap-server $S --topic users --group user-service --from-beginning > /dev/null 2>&1 &

# notification-service: baca semua
timeout $T $B/kafka-console-consumer.sh --bootstrap-server $S --topic notifications --group notification-service --from-beginning > /dev/null 2>&1 &

# log-analyzer: hanya 500 message (lag besar)
timeout $T $B/kafka-console-consumer.sh --bootstrap-server $S --topic logs --group log-analyzer --from-beginning --max-messages 500 > /dev/null 2>&1 &

wait
echo "Selesai."