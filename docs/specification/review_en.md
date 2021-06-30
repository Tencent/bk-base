# Reviewing for BK-BASE

We believe the values that code review brings: Code review can improve not only the quality and readability of the code, but also the abilities and coding skills of developers, so that better designs can be produced.

Below are relevant instructions on review issues and PRs of the BlueKing team.

## Welcome PRs {#welcome-prs}

The most important rule: problem reporting and feature requests are welcome at any time.

Please review [commit-spec](./commit-spec_en.md) for relevant commit specifications.

## Code Reviewers {#code-reviewers}

Code review may slightly delay the announcement of features and bug fixes as well as create extra review work. Therefore, the BlueKing team hopes that relevant personnel actively involved in the project are also active reviewers. Meanwhile, relevant reviewers are expected to be equipped with professional knowledge in relevant fields, so that work efficiency can be improved.

## Review Details {#review-details}

After a PR is submitted, reviewers need to categorize this PR quickly. For example, they need to close duplicate requests, identify whether simple user error exists, and tag the request. They also need to confirm which reviewers are better equipped with professional knowledge to review this PR.

If the PR is denied, reviewers need to provide enough feedback information for the initiator and explains why it is closed. During the review process, the initiator of the PR should answer reviewers’ questions actively, make comments and adjust the submitted content properly if necessary.

Reviewers should process PRs in time on business days. On non-business days, it should be recognized that related processing will be delayed.

### Hold PR {#hold-pr}

It is impossible for all the personnel to process relevant requests all the time. If reviewers do not process the request immediately due to time limit, it is recommended to give feedbacks on processing time during the PR discussion. For the author of the PR, the reasonable practice is to actively negotiate a reasonable deadline with reviewers or the handover of the PR if other professional reviewers can process it.

## Merge {#Merge}

PR that meets the following requirements will be merged:

* Reviewers do not raise objections or make suggestions for adjustments.
* All the objections and suggestions for adjustments have been dealt with properly.
* Maintainers of at least one branch agree to merge.
* Relevant documents have been prepared and relevant tests have been passed.