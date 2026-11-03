package org.marketplace;

public final class App {
    private App() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        switch (args[0]) {
            case "shop" -> ShopProducer.sendAll();
            case "ban" -> BanCli.run(args);
            case "filter" -> ProductFilterApp.start();
            case "client" -> ClientCli.run(args);
            case "lake" -> LakeLoader.load();
            case "publish" -> RecommendationPublisher.publish();
            case "alerts" -> AlertLogger.serve();
            case "prove-acl" -> AclProbe.proveShopCannotWriteCleanTopic();
            default -> usage();
        }
    }

    private static void usage() {
        System.out.println("""
                marketplace.jar <команда>
                  shop
                  ban add <product_id>
                  ban remove <product_id>
                  ban list
                  filter
                  client search <название>
                  client recommend
                  lake
                  publish
                  alerts
                  prove-acl
                """);
    }
}
