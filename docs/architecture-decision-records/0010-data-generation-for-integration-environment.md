# 10. Data generation for Integration environment

**Date:** 2024-01-25

## Context

In the process of enhancing our test system, a decision had to be made regarding the approach to generating test data. The two options under consideration were:

- Generate random data based on a template.
- Anonymise live data and use it for testing purposes.

The team faced the challenge of determining the most suitable and effective method to ensure comprehensive testing.

## Decision

After careful consideration and evaluating the pros and cons of each approach, the team has decided to anonymise live data for use in the test system.

### Rationale

The following factors influenced the decision:

- Realism: Anonymising live data allows us to work with actual scenarios, ensuring that the test environment closely mirrors real-world conditions. This enhances the realism of our testing scenarios and increases the likelihood of identifying potential issues that may arise in a production setting.
- Data Relevance: Live data, even when anonymised, retains the essential characteristics of actual production data. This ensures that the test cases are based on meaningful and representative information, leading to more accurate evaluations of system performance.
- Scenario Coverage: Utilising anonymised live data enables us to cover a broader range of scenarios, including edge cases and uncommon situations that may not be adequately captured by randomly generated data.

## Consequences

While anonymising live data presents several advantages, it introduces several negative consequences:

- Divergence between Test and Production Systems: The integration system now has a different architecture to production as a result of the anonymiser in circuit. This divergence could impact the accuracy of certain tests and potentially lead to scenarios not accurately representing the production environment.
- Developer Time and Effort: Implementing the anonymisation process requires additional developer time to write and maintain the code responsible for anonymising live data, as well as managing dependency updates to mitigate the risk of security issues in out-of-date dependencies. This could potentially divert resources from other critical development tasks, impacting overall project timelines.
- Effort for Managing Divergence: The team will need to expend effort to manage and mitigate the divergence between the test and production systems. This includes monitoring for potential discrepancies and implementing strategies to address any issues that arise.

### Future Considerations

As the project progresses, the team will need to carefully balance the benefits of using anonymised live data with the associated costs, including developer time and effort. Strategies for optimising the anonymisation process and maintaining alignment between the test and production environments should be continuously evaluated.
