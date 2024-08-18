# capstone-project

## Objective
- To provide a comprehensive analysis of H1-B and Permanent visa applications by examining the distribution of applications across various states and job titles, identifying the most common job roles, understanding salary trends, and evaluating approval rates for specific job titles. This analysis aims to offer valuable insights for prospective applicants and employers by highlighting key patterns and trends in visa applications.

## Questions of Analysis
- How are H1-B and Permanent visa applications distributed across different worksiteState?
- What are the most common job titles and roles for H1-B and Permanent visa applicants?
- How are H1-B and Permanent visa applications distributed across different job titles?
- What are the average salaries offered by companies for H1-B and Permanent visa positions?
- What are the approval rates for specific job titles?

## Source Datasets
| Source Name                             | Source Type | Source Documentation                                               |
|-----------------------------------------|-------------|--------------------------------------------------------------------|
| Stock-visa-application (Finnhub)        | API         | [Documentation](https://finnhub.io/docs/api/stock-visa-application)|
| Stream real-time trades for US stocks (Finnhub) | Websockets  | [Documentation](https://finnhub.io/docs/api/websocket-trades)      |
| Nasdaq stock name                       | CSV         | [GitHub Repository](https://github.com/datasets/nasdaq-listings/blob/master/data/nasdaq-listed.csv) |

## Solution Architect
![solution-architect](images/solution-architect.jpg)

## Steps of Implementation

### 1. Source: Finhub API/Finhub WebSockets
- The data source application is designed to fetch financial data from two primary sources provided by Finnhub:
    -  Stock VISA Application Data: This data source is accessed via the Finnhub API, which provides historical financial transaction data related to H1-B and Permanent visa applications for companies that existed in the US stock markets.
    - Real-time Trades for US Stocks: To capture live trade events, the application uses Finnhub's WebSocket API, which streams real-time trade data for US stocks. By subscribing to this WebSocket service, the application receives immediate updates on trade activities for the selected stocks (In this case, we have selected 10 companies within Nasdaq), enabling it to process and analyze real-time market data as it happens.
- Implementation of data streaming

