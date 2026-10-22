# Published workflow identity consumer

This fixture normally installs the selected published Python SDK and compares
its installed source with the exact immutable release. Set the required Compose
environment values, use a uniquely named project and a disposable data/state
directory, then start the stack.

Run `/opt/venv/bin/python /fixture/consumer.py execute` in the client container,
followed by the same command with `cold` in a separate process. Every original
has a thirty-second whole observer and thirty-second run/execution deadlines.
The first workflow node, activity node and cold workflow node use separate
Worker instances and registrations. The final cold client verifies original
typed results and unchanged retained histories.

The eight originals cover default/named namespaces, literal numeric workflow
IDs, activity suspension/cold replay, and continue-as-new successor identity.
Continue-as-new result inspection explicitly pins the successor run; it does
not treat an original run handle as a promise to follow a chain.

Stop and audit the API/database, then remove the uniquely named project and all
task-owned data, dependencies and raw output. The fixture does not qualify any
native runtime or a broader SDK contract.
