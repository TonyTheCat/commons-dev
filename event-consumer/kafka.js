"use strict";

const kafka = require("no-kafka");
const Promise = require("bluebird");

/**
 * @param config - kafka config
 * @param eventHandler - event handler
 * @param errorHandler - error handler
 */
const newConsumer = (config, eventHandler, errorHandler) => {
    const dataHandler = (messageSet, topic, partition) => {
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

    function generateClientID() {
        let text = "";
        const possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for (let i = 0; i < 10; i++) {
            text += possible.charAt(Math.floor(Math.random() * possible.length));
        }
        return text;
    }

    let consumer = new kafka.GroupConsumer(
        {
            connectionString: config.kafka.hosts.join(","),
            groupId: config["consumer-group"],
            clientId: generateClientID()
        }
    );
    const strategies = [{
        subscriptions: [config.kafka.topic],
        handler: dataHandler
    }];
    return consumer.init(strategies);
};

module.exports = newConsumer;
