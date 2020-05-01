#!/bin/bash
cp /root/.cache/*_jar.jar ./zetasql-kotlin/build
cd zetasql-kotlin
./gradlew jibDockerBuild --image=zetasql-formatter
/bin/bash
