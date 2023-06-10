#A brief principle  of how it works

``` mermaid
graph 
  B{<br>  <br>Pool <br>idle.<br> <br>  <br>  };
  B -->|V.Mem<br>available| A[Allocates V.Mem];
  A --> C[Launches job];
  E[Job submited] -->L[Pre-processing];
  L-->B;
  Z(User submits<br>jobs continuously) -..->|Only in dynamic pool| E;
  B --->|For every job| F[Job process checking];
  F --->|Job is dead| G[Release V.Mem];
  F --->|Job is alive| B;
  G --> K[Post-processing];
  K --> B;
  B -->|Emit status| H[Displays current<br>jobs & pool status];
  B -...->|Static Pool| R[Terminates when all<br>jobs are done];
  B -...->|Dynamic Pool| U[Terminates when *end*<br>method is called];
```
