# TODO

- Conveyare :produce shouldn't be necessary when send-message is called
- Conveyare send should have schema check (when outside router)
- Fix conveyare stop - (Thread/sleep 1000) ; Because conveyare doesn't seem to guarantee all are sent when stopping


- TODO topics are implicit from handler definition?
- TODO different topics have different middleware? middleware can detect that anyway
- TODO different topics have different threads so that one doesn't overwhelm / block the other

- TODO make routes more performant by compiling schema checks etc upfront

- TODO split send from receive paths, too complex
- TODO rethink everything
- TODO allow users to get to the fundamentals, or supply fundamentals, after all what is this for?
- consistent logging
- easy to get started (but it's not)
- tools for routing, sending
- wrap samp (except it doesn't, should wrap by default but allow it to be excluded)
- TODO remove schema
- TODO needs to register UUID for responses
- TODO review pedestal architecture


- TODO decouple transport to test

spec for config / opts

transport - swappable backend
configure bus
write message to bus
read messages from bus
:send-map
:receive-map

stage
:name
:send
:receive

router-stage
[[action destructuring fn]]
