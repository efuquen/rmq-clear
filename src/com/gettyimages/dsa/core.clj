(ns com.gettyimages.dsa.core
    (:gen-class :main true)
    (:require [langohr.core      :as rmq]
              [langohr.channel   :as lch]
              [langohr.queue     :as lq]
              [langohr.consumers :as lc]
              [langohr.basic     :as lb]
              [clojure.edn       :as edn]))

(defn message-handler
    [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
    (println (format "[consumer] Received a message: %s, delivery tag: %d, content type: %s, type: %s"
                                        (String. payload "UTF-8") delivery-tag content-type type)))

(defn read-edn-file
  [config-file]
  (with-open [config-reader (java.io.PushbackReader. (java.io.FileReader. config-file))] 
    (edn/read config-reader)
))

(defn -main
  [config-file qname sleep-time & args]
  (let [config (read-edn-file config-file)
        conn (rmq/connect config)
        ch (lch/open conn)]
  (try 
    (do
      (lc/subscribe ch qname message-handler :auto-ack true)
      (println (str "[main] Sleep ", sleep-time))
      (Thread/sleep (Integer/parseInt sleep-time))
      (println "[main] Disconnecting...")
    )
    (finally
      (rmq/close ch)
      (rmq/close conn)
  ))
))
