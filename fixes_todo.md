# Todo: Order Management System Fixes and Upgrades

This todo file outlines the tasks and phases required to upgrade the Java version, optimize the Kafka setup, implement E2E testing with Testcontainers, and integrate SonarQube code quality checks in CI.

---

## Coding Agent Workflow Instructions

All developer subagents must strictly adhere to the following workflow for executing individual tasks:

1. **Git Branching**:
   - Create and switch to a new local branch named exactly after the task ID: `git checkout -b task/<task-id>` (e.g., `git checkout -b task/P1-01`).
   
2. **Code Implementation & Verification**:
   - Write clean, modular, and well-packaged code.
   - Perform local verification of the implemented feature (run tests, verify builds, etc.).
   
3. **PR Creation**:
   - Commit changes: `git commit -am "Implement <task-id>: <short description>"`
   - Push to origin: `git push origin task/<task-id>`
   - Create a Pull Request using GitHub CLI: `gh pr create --title "task/<task-id>: Implementation" --body "Closes <task-id>"`
   
4. **Code Review Loop**:
   - Invoke a reviewer subagent to review the PR. The reviewer must inspect the code and comment on the GitHub PR if issues are found, but must not make code edits directly.
   - Address any reviewer comments by committing and pushing updates to the same branch.
   
5. **PR Merging & Cleanup**:
   - Once approved, merge the PR into the main branch (e.g., using squash merge: `gh pr merge --squash --delete-branch`).
   - Switch back to the main branch locally and pull the latest changes: `git checkout main && git pull`.
   - Clean up local branches: `git branch -D task/<task-id>`.

6. **Final Coordinator Output**:
   - After completing a task, the subagent must output a JSON message formatted as follows:
     ```json
     {
       "taskId": "<task-id>",
       "status": "completed",
       "branchMerged": "task/<task-id>",
       "summary": "<brief summary of what was accomplished>"
     }
     ```

---

## Phases & Tasks

### Phase 1: Java 21 Upgrade & Modernization

- [x] **P1-01**: Upgrade Java version to 21.
  - Update `pom.xml` (or build configuration) to use Java 21 target/source options.
  - Fix any deprecated dependencies or plugins that are incompatible with Java 21.
  
- [x] **P1-02**: Modernize code utilizing Java 21 features.
  - Refactor data models (e.g. `OrderPlaced`) to use Java 21 record patterns or pattern matching where appropriate.
  - Update compiler settings to enforce modern Java best practices.

- [x] **P1-EC**: Phase 1 Exit Check:
  - Run `./mvnw clean package` (or `./gradlew build`) using Java 21 and verify the build passes with no errors.
  - Verify that unit tests run and pass on Java 21.

---

### Phase 2: Kafka Setup & Spring Annotation Improvements

- [x] **P2-01**: Use annotations for Kafka, Spring, and Lombok.
  - Refactor all configurations, producers, and consumers to utilize Lombok annotations (e.g. `@RequiredArgsConstructor`, `@Slf4j`) to remove boilerplate code.
  - Replace manual JSON serialization/deserialization beans with standard Spring Boot / Jackson properties.
  
- [x] **P2-02**: Improve Kafka configurations.
  - Optimize consumer and producer settings (e.g., set connection timeouts, fetch sizes, key/value serializers/deserializers configuration in `application.yml`).
  - Configure consumer concurrency settings to support the partition structure.

- [x] **P2-EC**: Phase 2 Exit Check:
  - Start Kafka and verify that the Spring Boot application starts without configuration errors.
  - Send sample messages to ensure producer and consumer function as expected with the improved annotations and properties.

---

### Phase 3: E2E Testing with Testcontainers

- [x] **P3-01**: Add Testcontainers dependencies.
  - Include Testcontainers BOM and the Kafka Testcontainers library in `pom.xml` (or build configuration).
  
- [x] **P3-02**: Implement E2E Kafka Integration Test.
  - Create a new integration test class using `@SpringBootTest` and Testcontainers to start a Kafka broker dynamically.
  - Implement full E2E flows verifying order placement, consumption, serialization/deserialization, and dead-letter queue (DLT) fallback.

- [x] **P3-EC**: Phase 3 Exit Check:
  - Execute `./mvnw test` locally and verify that the integration test starts the Testcontainers Kafka instance, runs the tests, and completes successfully.

---

### Phase 4: SonarQube CI Quality Gates

- [ ] **P4-01**: Integrate SonarQube checks in CI workflow.
  - Add SonarQube / SonarCloud scanner step to `.github/workflows/ci.yml`.
  - Configure Maven/Gradle plugins to support code coverage (using JaCoCo) and SonarQube reporting.

- [ ] **P4-EC**: Phase 4 Exit Check:
  - Validate the updated `.github/workflows/ci.yml` syntax.
  - Verify that the SonarQube scan configuration works correctly with build files.
