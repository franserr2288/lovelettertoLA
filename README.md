
Dev Notes: 
Need to do more research to get an idea of where soft/hard power is in the city, I don't know the exact topology other than the obvious positions. Hopefully open data sources expose their info or I'll have to scrape them = brittle


cicd pipelines are stood up, only infra that is tightly coupled to whatever features i want will be necessary now. I don't want more shared infra, annoying to manage. 

finish manual testing for ingestion, see if you need to change consumption pattern 

for now we are good, but i'm close to that 3gb memory limit.. used 2078mb out of 3008... I can build it in batches but would probably take another hour or two. This is good enough for now, will see if other datasets force my hand