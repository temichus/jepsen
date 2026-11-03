(ns scylla.sc.elle-patch
  "Fixes a false :cyclic-versions in elle.rw-register 0.2.5 and 0.2.6 with
  :linearizable-keys? true. downstream-ops-by-ext-key-transitive! puts a
  successor's memoized per-key LinearSet into the new downstream map as is;
  downstream-ops-by-ext-key-local! then bs/adds to it, mutating the shared
  set, so earlier ops appear to follow later ones and Elle infers version
  orders the history does not have. We put a linear copy instead.

  Fixed upstream in Elle 0.2.7 (5cc2eca, \"rw could incorrectly infer
  version order cycles\": the local fn starts from an immutable Set), but
  0.2.7+ ships JDK 21 classes and Debian 12 has 17. Drop this namespace
  when Jepsen and Elle can move to 0.2.7+."
  (:require [elle [graph :as g]
                  [rw-register :as r]])
  (:import (io.lacuna.bifurcan IEntry IMap ISet)))

(alter-var-root
  #'r/downstream-ops-by-ext-key-transitive!
  (fn [_]
    (fn [^IMap downstream ^IMap _kg ^ISet ext-keys ^IMap transitive-downstream]
      (reduce (fn [^IMap downstream ^IEntry e]
                (if (.contains ext-keys (.key e))
                  downstream
                  (.put downstream (.key e)
                        (.linear (.forked ^ISet (.value e)))
                        g/union-bset)))
              downstream
              transitive-downstream))))
