# Contributing Guide
First off, thanks for taking the time to contribute! 🎉

The following is a set of guidelines for contributing to tapdata. These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.

## How to Contribute
To successfully run this open-source project, there are three key repositories you need to focus on:

1. **Main Project Repository**: 
   - URL: [https://github.com/tapdata/tapdata](https://github.com/tapdata/tapdata)
   - This repository contains the manager backend service, and engine service which run real tasks

2. **Frontend Repository**: 
   - URL: [https://github.com/tapdata/tapdata-web](https://github.com/tapdata/tapdata-web)
   - This repository contains the code for the frontend application of the project, including all UI components.

3. **Connectors Repository**: 
   - URL: [https://github.com/tapdata/tapdata-connectors](https://github.com/tapdata/tapdata-connectors)
   - This repository contains all connectors supported by tapdata service

### Guidelines
1. If you have any ideas, create an issue in the corresponding repository. This issue can be a bug report or a feature suggestion.
2. Fork the corresponding project to your own space.
3. Create a branch in your project space. The branch name should start with the issue number from the first step, such as `25-fix-xxx`.
4. Complete your code submission and self-test to ensure the changes achieve the desired modification.
5. Submit a PR to the `develop` branch of the original project.
6. In the PR review, please describe through comments how the modification should be validated.
7. Wait for the PR review.

During the PR review, we will follow these rules:
1. First, we will perform a static code check to ensure the code changes are logically sound.
2. Second, we will compile the entire project using your modified code and conduct basic regression testing, which includes about 100 test cases, to ensure that no fundamental quality issues are introduced.
3. Third, we will add a test case for the part of the modification that needs to be verified and test its stable changes before and after the code modification.
4. Finally, we will merge your changes.

The time required for code merging depends on the daily work schedule of the development team, and we cannot guarantee a specific timeframe. If you have an urgent need for the PR to be merged, please feel free to remind us in the group.
