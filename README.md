Dev Notes: 
another architecture change, after ingestion I was fanning out all the partitions into the next processing layer in order to kick off the layer. the fan out and the processing was not an issue until I had to fan-in the workloads, I had no way of knowing if all jobs working in parallel were finished unless i swapped to step functions (want to avoid.. clunky)  or unless in introduced more infra with a centralized counter that can wait for all of the other messages to get there. I don't have a need for parallelism right now, so instead the ingestion message will kick off a single message that will hold all the ids of the jobs i need it to do, each invocation will pop one off then requeue work. this is a recursive sqs pattern, was going to use this pattern to ingest if i had to change the consumption pattern. 

previous architecture changes, going to break up datasets at ingestion layer instead of making a downstream processor do it or to filter or do request streaming. easier to handle now, will break it up by some partition column, in the case of city 311 i will use a council district number since zipcode cardinality = headache. now the only memory bottle neck will be the ingestion piece, if it can get past my ingestion then i have a guarantee it will get past the processing layer(s)

I have initial abstractions for stakeholders, but I need their personal info. need to see if it is exposed, otherwise i'll keep a json file that I update manually. ideally not


cicd pipelines are stood up, only infra that is tightly coupled to whatever features i want will be necessary now. I don't want more shared infra, annoying to manage. 


for now we are good w the ingestion component, but i'm close to that 3gb memory limit.. used 2078mb out of 3008... I can build it in batches but would probably take another hour or two. This is good enough for now, will see if other datasets force my hand


Next steps: 
- make it explicit what app components are getting what s3 prefix locations, manageable for now but will become annoying once you add more components
- finish testing event ledger api now that it is deployed
- do more research on the next phase, not sure how I want the architecture to go there. I want to enable ML on the datasets later once I produce enough findings, but I would need more dimensions in the events for me to find correlations across datasets' event emission.
- For now I know that I want my first processing layer to kick off when the raw parquets have been loaded into the bucket, I am going to make the ingestion piece send the next queue message to the processing layer.
- Find more valueable datasets and test your ingestion subsystem against them 
- Some datasets are split by year over different endpoints (city311), I don't know the extent of the differences in their columns/labels/values. I have faith in their engineers, but if it's a mess then you'll need a transformation component to get a coherent dataset across years. I would also need to figure out if or how I want to stitch that data on my own side of things... a lot of answers I have to chase down. Weigh potential value of historial analysis over years against lack of it, or effort of patchwork engineering at the processing/investigative phase to get that insight
- If you find datasets where you hit your ceiling on the ingestion function, then you build a new version but don't waste time on it yet 


https://myla311.lacity.gov/s/ the main page for the 311 data