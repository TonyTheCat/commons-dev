"use strict";

const kafka = require("no-kafka");
const Promise = require("bluebird");

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */
function newConsumer(config, eventHandler, errorHandler) {
    var dataHandler = (messageSet, topic, partition) => {
        return Promise.all(messageSet.map(message =>
            eventHandler(JSON.parse(message.message.value.toString("utf8")))
        ))
            .then(() => {
                return Promise.each(messageSet, m => {
                    return consumer.commitOffset({ topic: topic, partition: partition, offset: m.offset, metadata: "optional" });
                });
            })
            .catch(err => {
                errorHandler(err);
            });
    };
    var consumer = new kafka.GroupConsumer(
        {
            connectionString: config.kafka.hosts.join(","),
            groupId: config["consumer-group"],
            clientId: config.kafka["client-id"]
        }
    );
    var strategies = [{
        subscriptions: [config.kafka.topic],
        handler: dataHandler
    }];
    return consumer.init(strategies);
}
module.exports = newConsumer;
