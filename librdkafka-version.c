#include <librdkafka/rdkafka.h>
#include <stdio.h>

int main() {
  printf("%s\n", rd_kafka_version_str());
  return 0;
}
