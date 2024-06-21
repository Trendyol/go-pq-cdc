## Contribution Guidelines

Thank you for your interest in go-pq-cdc!

This project welcomes contributions and suggestions. Most contributions require you to signoff on your commits. Please
follow the instructions provided below.

Contributions come in many forms: submitting issues, writing code, participating in discussions and community calls.

This document provides the guidelines for how to contribute to the project.

### Issues

This section describes the guidelines for submitting issues

**Issue Types**

There are 2 types of issues:

- Issue/Bug: You've found a bug with the code, and want to report it, or create an issue to track the bug.
- Issue/Feature: You have something on your mind, which requires input form others in a discussion, before it eventually
  manifests as a proposal.

### Before You File

Before you file an issue, make sure you've checked the following:

1. Check for existing issues
   Before you create a new issue, please do a search in open issues to see if the issue or feature request has already
   been filed.

If you find your issue already exists, make relevant comments and add your reaction. Use a reaction:
👍 up-vote
👎 down-vote

2. For bugs
   Check it's not an environment issue. For example, if your configurations correct or network connections is alive.

### Contributing to go-pq-cdc

Pull Requests
All contributions come through pull requests. To submit a proposed change, we recommend following this workflow:

- Make sure there's an issue (bug or feature) raised, which sets the expectations for the contribution you are about to
  make.
- Fork the relevant repo and create a new branch
- Create your change
- Code changes require tests
- Update relevant documentation for the change
- Commit sign-off and open a PR
- Wait for the CI process to finish and make sure all checks are green
- A maintainer of the project will be assigned, and you can expect a review within a few days

### Use work-in-progress PRs for early feedback

A good way to communicate before investing too much time is to create a "Work-in-progress" PR and share it with your
reviewers. The standard way of doing this is to add a "[WIP]" prefix in your PR's title and assign the do-not-merge
label. This will let people looking at your PR know that it is not well baked yet.

### Developer Certificate of Origin: Signing your work

**Every commit needs to be signed**

The Developer Certificate of Origin (DCO) is a lightweight way for contributors to certify that they wrote or otherwise
have the right to submit the code they are contributing to the project. Here is the full text of the DCO, reformatted
for readability:

By making a contribution to this project, I certify that:

    (a) The contribution was created in whole or in part by me and I have the right to submit it under the open source license indicated in the file; or

    (b) The contribution is based upon previous work that, to the best of my knowledge, is covered under an appropriate open source license and I have the right under that license to submit that work with modifications, whether created in whole or in part by me, under the same open source license (unless I am permitted to submit under a different license), as indicated in the file; or

    (c) The contribution was provided directly to me by some other person who certified (a), (b) or (c) and I have not modified it.

    (d) I understand and agree that this project and the contribution are public and that a record of the contribution (including all personal information I submit with it, including my sign-off) is maintained indefinitely and may be redistributed consistent with this project or the open source license(s) involved.

Contributors sign-off that they adhere to these requirements by adding a Signed-off-by line to commit messages.

This is my commit message

    Signed-off-by: Random X Developer <random@developer.example.org>

Git even has a -s command line option to append this automatically to your commit message:

    $ git commit -s -m 'This is my commit message'

Each Pull Request is checked whether or not commits in a Pull Request do contain a valid Signed-off-by line.

I didn't sign my commit, now what?!
No worries - You can easily replay your changes, sign them and force push them!

    git checkout <branch-name>
    git commit --amend --no-edit --signoff
    git push --force-with-lease <remote-name> <branch-name>

### Use of Third-party code

- Third-party code must include licenses.

**Thank You!** - Your contributions to open source, large or small, make projects like this possible. Thank you for
taking the time to contribute.

### Code of Conduct

This project has adopted the Contributor Covenant Code of Conduct