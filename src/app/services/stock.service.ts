import { HttpClient } from '@angular/common/http';
import { Injectable, OnDestroy } from '@angular/core';
import { BehaviorSubject, forkJoin, Observable, takeUntil } from 'rxjs';
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import { SubscriptionBase } from '../core/subscription-base';
import { IStock } from '../models/stock.model';

@Injectable({
  providedIn: 'root'
})
export class StockService extends SubscriptionBase implements OnDestroy {
  private readonly symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA'];
  private readonly stocks: string[] = ['Apple Inc.', 'Alphabet Inc.', 'Microsoft Corporation', 'Tesla, Inc.'];
  private readonly BASE_URL = 'https://finnhub.io/api/v1';
  private readonly FINNHUB_API_KEY = 'd7btmopr01quh9fbnt2gd7btmopr01quh9fbnt30';
  private readonly MASSIVE_API_KEY = '6aO9LWfZ3C5JDFiW1_1Q1VpwXbhtrHNC';

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private socket$!: WebSocketSubject<any>;
  private stocksSubject = new BehaviorSubject<IStock[]>([]);
  private lastYearInterval: number;
  private symbolsLastYearLow = Array(this.symbols.length).fill(0);
  private symbolsLastYearHigh = Array(this.symbols.length).fill(0);

  public stocks$: Observable<IStock[]> = this.stocksSubject.asObservable();

  constructor(
    private http: HttpClient
  ) {
    super();

    this.lastYearInterval = setInterval(() => {
      this.updateYearlyPrices();
    }, (24 * 60 * 60 * 1000)); // Update every 24 hours

    this.initValues();
    this.connectWebSocket();
  }

  ngOnDestroy(): void {
    clearInterval(this.lastYearInterval);
    this.destroySubs();
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private getYearlyPrices(): Observable<any[]> {
    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);

    const from = oneYearAgo.toISOString().split('T')[0];
    const to = new Date().toISOString().split('T')[0];

    const quoteRequests = this.symbols.map(symbol =>
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      this.http.get<any>(
        `https://api.massive.com/v2/aggs/ticker/${symbol}/range/1/day/${from}/${to}?adjusted=true&sort=asc&limit=50000&apiKey=${this.MASSIVE_API_KEY}`
      )
    );

    return forkJoin(quoteRequests);
  }


  private updateYearlyPrices(): void {
    this.getYearlyPrices().subscribe({
      next: async (yearlyPrices) => {
        await this.updateYearlyHighLow(yearlyPrices);
        const currentStocks: IStock[] = this.stocksSubject.value;
        this.symbols.map((symbol: string, index: number) => {
          currentStocks.filter(s => s.symbol === symbol).map(stock => {
            stock.fiftyTwoWeekHigh = this.symbolsLastYearHigh[index];
            stock.fiftyTwoWeekLow = this.symbolsLastYearLow[index];
          });
        });
        this.stocksSubject.next(currentStocks);
        console.log('✅ Yearly high/low updated');
      }
    });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private fetchInitialQuotes(): Observable<any[]> {
    const quoteRequests = this.symbols.map(symbol =>
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      this.http.get<any>(
        `${this.BASE_URL}/quote?symbol=${symbol}&token=${this.FINNHUB_API_KEY}`
      )
    );

    return forkJoin(quoteRequests);
  }

  private initValues(): void {
    forkJoin([this.getYearlyPrices(), this.fetchInitialQuotes()]).subscribe({
      next: async ([yearlyPrices, initialData]) => {
        await this.updateYearlyHighLow(yearlyPrices);
        this.initializeStocks(initialData);
      },
      error: (err) => console.error('Error fetching data', err)
    });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async updateYearlyHighLow(yearlyPrices: any): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    yearlyPrices.forEach((data: { results: any[]; }, index: number) => {
      if (!data.results || data.results.length === 0) {
        return;
      }

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const highs = data.results.map((d: any) => d.h);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const lows = data.results.map((d: any) => d.l);
      this.symbolsLastYearHigh[index] = Math.max(...highs);
      this.symbolsLastYearLow[index] = Math.min(...lows);
    });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private initializeStocks(responses: any): void {
    const currentMap: IStock[] = [];

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    responses.forEach((quote: any, index: number) => {
      const symbol = this.symbols[index];
      if (quote && typeof quote.c === 'number') {
        const stock: IStock = {
          symbol,
          name: this.stocks[index].charAt(0).toUpperCase() + this.stocks[index].slice(1),
          currentPrice: quote.c,
          dailyHigh: quote.h,
          dailyLow: quote.l,
          fiftyTwoWeekHigh: this.symbolsLastYearHigh[index],
          fiftyTwoWeekLow: this.symbolsLastYearLow[index],
          isActive: true,
          direction: 'neutral',
          lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
        };
        currentMap.push(stock);
      }
    });

    this.stocksSubject.next(currentMap);
    console.log('✅ Initial stock quotes loaded from REST API');
  }

  private connectWebSocket(): void {
    const url = `wss://ws.finnhub.io?token=${this.FINNHUB_API_KEY}`;
    this.socket$ = webSocket({
      url,
      openObserver: {
        next: () => {
          console.log('✅ Finnhub WebSocket connected');
          this.subscribeToSymbols();
        }
      }
    });

    this.socket$.pipe(takeUntil(this.destroy$)).subscribe({
      next: (msg) => this.handleMessage(msg),
      error: (err) => console.error('WebSocket error:', err),
      complete: () => console.warn('WebSocket closed')
    });
  }

  private subscribeToSymbols(): void {
    this.symbols.forEach(symbol => {
      this.socket$.next({ type: 'subscribe', symbol });
    });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private handleMessage(msg: any): void {
    if (msg.type === 'trade' && msg.data) {
      let updatedMap: IStock[] = [];
      const currentMap = this.stocksSubject.value;

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      msg.data.forEach((trade: any) => {
        const symbol: IStock = currentMap.find(s => s.symbol === trade.s) || {
          symbol: trade.s,
          name: trade.s,
          currentPrice: 0,
          dailyHigh: 0,
          dailyLow: 0,
          fiftyTwoWeekHigh: 0,
          fiftyTwoWeekLow: 0,
          isActive: true,
          direction: 'neutral',
          lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
        };

        if (!symbol.isActive) {
          return;
        }

        const oldPrice = symbol?.currentPrice ?? 0;
        const direction = trade.p > oldPrice ? 'up' : trade.p < oldPrice ? 'down' : 'neutral';
        const stock: IStock = {
          ...symbol,
          symbol: trade.s,
          currentPrice: trade.p,
          dailyHigh: Math.max((symbol?.dailyHigh ?? 0), trade.p),
          dailyLow: Math.min((symbol?.dailyLow || trade.p), trade.p),
          fiftyTwoWeekHigh: this.symbolsLastYearHigh[this.symbols.indexOf(trade.s)] || trade.p,
          fiftyTwoWeekLow: this.symbolsLastYearLow[this.symbols.indexOf(trade.s)] || trade.p,
          direction,
          lastUpdated: new Date(trade.t).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
        };
        updatedMap = currentMap.map(s => s.symbol === trade.s ? stock : s);
      });

      this.stocksSubject.next(updatedMap);
    }
  }

  public toggleStock(ticker: string): void {
    const currentMap = this.stocksSubject.value;
    const stock = currentMap.find(s => s.symbol === ticker);
    if (!stock) return;

    stock.isActive = !stock.isActive;
    if (!stock.isActive) {
      stock.direction = 'neutral';
    }

    const updatedMap = currentMap.map(s => s.symbol === ticker ? stock : s);
    this.stocksSubject.next(updatedMap);
  }
}