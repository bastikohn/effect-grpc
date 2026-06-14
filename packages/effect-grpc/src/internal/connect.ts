import { createClient, type Client } from "@connectrpc/connect";
import type { DescService } from "@bufbuild/protobuf";
import type { Transport } from "@connectrpc/connect";

const clients = new WeakMap<
  Transport,
  WeakMap<DescService, Client<DescService>>
>();

export const getClient = (
  transport: Transport,
  service: DescService,
): Client<DescService> => {
  let byService = clients.get(transport);
  if (!byService) {
    byService = new WeakMap();
    clients.set(transport, byService);
  }
  let client = byService.get(service);
  if (!client) {
    client = createClient(service, transport);
    byService.set(service, client);
  }
  return client;
};
