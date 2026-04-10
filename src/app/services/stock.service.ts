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
  private readonly API_KEY = 'd7btmopr01quh9fbnt2gd7btmopr01quh9fbnt30';

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private socket$!: WebSocketSubject<any>;
  private stocksSubject = new BehaviorSubject<IStock[]>([
    // {
    //   symbol: 'AAPL',
    //   name: 'Apple Inc.',
    //   dailyHigh: 0,
    //   dailyLow: 0,
    //   isActive: true,
    //   direction: 'neutral',
    //   lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
    // },
    // {
    //   symbol: 'GOOGL',
    //   name: 'Alphabet Inc.',
    //   dailyHigh: 0,
    //   dailyLow: 0,
    //   isActive: true,
    //   direction: 'neutral',
    //   lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
    // },
    // {
    //   symbol: 'MSFT',
    //   name: 'Microsoft Corporation',
    //   dailyHigh: 0,
    //   dailyLow: 0,
    //   isActive: true,
    //   direction: 'neutral',
    //   lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
    // },
    // {
    //   symbol: 'TSLA',
    //   name: 'Tesla, Inc.',
    //   dailyHigh: 0,
    //   dailyLow: 0,
    //   isActive: true,
    //   direction: 'neutral',
    //   lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
    // }
  ]);

  public stocks$: Observable<IStock[]> = this.stocksSubject.asObservable();

  constructor(
    private http: HttpClient
  ) {
    super();
    this.fetchInitialQuotes();
    this.connectWebSocket();
  }

  ngOnDestroy(): void {
    this.destroySubs();
  }

  private fetchInitialQuotes(): void {
    const quoteRequests = this.symbols.map(symbol =>
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      this.http.get<any>(
        `${this.BASE_URL}/quote?symbol=${symbol}&token=${this.API_KEY}`
      )
    );
    
    forkJoin(quoteRequests).subscribe({
      next: (responses) => {
        const currentMap: IStock[] = [];

        responses.forEach((quote, index) => {
          const symbol = this.symbols[index];
          if (quote && typeof quote.c === 'number') {
            const stock: IStock = {
              symbol,
              name: this.stocks[index].charAt(0).toUpperCase() + this.stocks[index].slice(1),
              currentPrice: quote.p,
              dailyHigh: quote.p,
              dailyLow: quote.p,
              fiftyTwoWeekHigh: quote.p,
              fiftyTwoWeekLow: quote.p,
              isActive: true,
              direction: 'neutral',
              lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
            };
            currentMap.push(stock);
          }
        });

        this.stocksSubject.next(currentMap);
        console.log('✅ Initial stock quotes loaded from REST API');
      },
      error: (err) => {
        console.error('❌ Failed to fetch initial quotes:', err);
      }
    });
  }

  private connectWebSocket(): void {
    const url = `wss://ws.finnhub.io?token=${this.API_KEY}`;
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
          fiftyTwoWeekHigh: Math.max((symbol?.fiftyTwoWeekHigh ?? 0), trade.p),
          fiftyTwoWeekLow: Math.min((symbol?.fiftyTwoWeekLow || trade.p), trade.p),
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