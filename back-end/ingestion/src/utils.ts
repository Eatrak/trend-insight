import { Kafka, Producer } from "kafkajs";

/**
 * Shared utility methods for the ingestion service.
 */
export class Utils {
  /**
   * Pauses the execution for a given number of milliseconds.
   */
  static sleep = (ms: number) =>
    new Promise((resolve) => setTimeout(resolve, ms));

  /**
   * Connects and returns a Kafka Producer. Retries indefinitely if the broker is unavailable.
   */
  static async getProducer(kafka: Kafka): Promise<Producer> {
    const producer = kafka.producer();
    while (true) {
      try {
        await producer.connect();
        return producer;
      } catch (e: any) {
        console.error(
          `[ingestion] Kafka connection failed: ${e.message}. Retrying...`,
        );
        await Utils.sleep(5000);
      }
    }
  }

  /**
   * Clamps a number between a minimum and maximum value.
   */
  static clamp(value: number, min: number, max: number): number {
    return Math.min(max, Math.max(min, value));
  }
}
