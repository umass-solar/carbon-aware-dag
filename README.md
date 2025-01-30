# Carbon-Aware Scheduling for Data Processing

As large-scale data-processing workloads continue to grow, their carbon footprint has become a concern. Prior research on carbon-aware scheduling has primarily focused on deferring tasks to better align with low-carbon energy availability, but these approaches are built on the underlying assumption that each task can be executed independently. 
In contrast, real-world data processing jobs have precedence constraints, where the outputs of one operation are the input of another. 
These dependencies complicate scheduling decisions, as delaying an upstream ``bottleneck'' task to take advantage of low-carbon energy may negatively affect downstream tasks, impacting the entire job completion time. 
In this paper, we show that carbon-aware scheduling of precedence-constrained tasks benefits from embedding the carbon cost of computations directly into an augmented scheduling policy. We introduce two carbon-aware frameworks $\texttt{DANISH}$ and $\texttt{CAP}$ that enable a configurable priority between reductions in carbon emissions and job completion time. 
Our main framework $\texttt{DANISH}$ is an interpretable layer that integrates with modern ML scheduler and explicitly considers DAG information in addition to carbon, while $\texttt{CAP}$ is a black-box resource provisioning module that is applicable to any underlying scheduling policy.
We give analytical results that characterize the trade-off between job completion time and carbon savings for both $\texttt{DANISH}$ and $\texttt{CAP}$.
Furthermore, our prototype integration with Spark and Kubernetes on a 100-node cluster shows that a moderate configuration of $\texttt{DANISH}$ reduces carbon footprint by up to 33.4\% without significantly increasing the end-to-end completion time for a batch of data processing jobs.

# README (split into simulator and other)
