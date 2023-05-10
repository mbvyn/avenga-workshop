FROM cassandra:4.0

COPY bike-cassandra.cql /docker-entrypoint-initdb.d/bike-cassandra.cql

COPY wrap.sh /wrap.sh

ENTRYPOINT ["/wrap.sh"]