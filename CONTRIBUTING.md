# Contributing
You can contribute with issues and pull requests (PRs). Simply logging issues for any problems you encounter is a great way to contribute. Contributing code is greatly appreciated.

## Where to report issues
New issues can be reported in the project [issue list](../../../issues).

Before logging a new issue, please search the list of issues to make sure it doesn't already exist.

If an existing issue for it does already exist, please include your own feedback in that existing issue's discussion and consider upvoting (👍 reaction) the original post. 

## Writing a good bug report
A good bug report makes it easier for maintainers to verify the issue and identify the root cause. Ideally, the following should be included:
- high level description of the problem
- a minimal reproduction of the issue
- description of the expected behaviour
- any known workarounds
- any other additional information you feel may be relevant

## Contributing changes
Project maintainers will merge accepted code changes from contributors.

## Dos and Don'ts
Do's:
- **DO** following the coding conventions and style that is in place in the project.
- **DO** include tests when adding new functionality.
- **DO** start by adding a test when addressing a bug, that highlights the current broken behaviour.
- **DO** help keep discussions on issues focused. When new or related topic comes up, consider raising a new issue rather than side-tracking another.
- **DO** prefer smaller PRs that cover single, discrete enhancements instead of large PRs that combine a wider set of changes.

Don'ts:
- **DON'T** submit PRs that alter licensing relating files or headers
- **DON'T** submit PRs that include sensitive details (passwords, personal data, proprietary information etc)

## Installation instructions

1. Fork/clone repo
2. Run `make install` to update pip, locally install the project, install pre-commit, and update it.

## Testing
### Integration Tests
The integration tests verify that each publicly exposed function is able to connect to its respective API or DPS service (and return a successful response where necessary). They are to be used alongside manual testing.

**To run integration tests locally**, you will need to have created a .env file in the root directory of the repository as follows:
```dotenv
INTEGRATION_TEST_ENACT_USERNAME=""
INTEGRATION_TEST_ENACT_API_KEY=""
INTEGRATION_TEST_FLEXTRACK_USERNAME=""
INTEGRATION_TEST_FLEXTRACK_API_KEY=""
```
And populate the entries with your credentials.

If you do not have credentials for one of these services (i.e. you have Enact credentials but not FLEXtrack), you can leave these empty and cease to run those integration tests. If you are making changes which affect a service you do not have credentials for, then testing should be done by somebody who has these credentials.

**Once you have set up your .env file:**
1. Run `pip install -e .[dev]` from your repository's root directory to ensure you have dev dependencies installed
2. Run `pytest` to execute all tests, or run `pytest tests/integration/{file_name}.py` to execute a particular suite of tests
