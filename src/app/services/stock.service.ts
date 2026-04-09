import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import { IStock } from '../models/stock.model';

@Injectable({
  providedIn: 'root'
})
export class StockService {
  private stocks: IStock[] = [
    {
      ticker: 'AAPL',
      name: 'Apple Inc.',
      currentPrice: 226.84,
      dailyHigh: 228.50,
      dailyLow: 225.10,
      fiftyTwoWeekHigh: 237.23,
      fiftyTwoWeekLow: 164.08,
      isActive: true,
      direction: 'neutral',
      lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
    },
    {
      ticker: 'GOOGL',
      name: 'Alphabet Inc.',
      currentPrice: 158.32,
      dailyHigh: 160.10,
      dailyLow: 157.45,
      fiftyTwoWeekHigh: 191.75,
      fiftyTwoWeekLow: 120.21,
      isActive: true,
      direction: 'neutral',
      lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
    },
    {
      ticker: 'MSFT',
      name: 'Microsoft Corporation',
      currentPrice: 421.65,
      dailyHigh: 425.00,
      dailyLow: 419.80,
      fiftyTwoWeekHigh: 468.35,
      fiftyTwoWeekLow: 345.00,
      isActive: true,
      direction: 'neutral',
      lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
    },
    {
      ticker: 'TSLA',
      name: 'Tesla, Inc.',
      currentPrice: 248.75,
      dailyHigh: 252.40,
      dailyLow: 245.10,
      fiftyTwoWeekHigh: 299.29,
      fiftyTwoWeekLow: 138.80,
      isActive: true,
      direction: 'neutral',
      lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
    }
  ];

  private stocksSubject = new BehaviorSubject<IStock[]>(this.stocks);
  public stocks$: Observable<IStock[]> = this.stocksSubject.asObservable();

  private updateInterval!: number | null;
  private ws: WebSocket | null = null;

  constructor() {
    this.startMockUpdates();
  }

  private startMockUpdates(): void {
    if (this.updateInterval) return;
    
    this.updateInterval = setInterval(() => {
      this.updatePrices();
    }, 3000);
  }

  private updatePrices(): void {
    const updated = this.stocks.map(stock => {
      if (!stock.isActive) {
        return { ...stock, direction: 'neutral' as const };
      }

      const volatility = (Math.random() * 3 - 1.5) / 100;
      const newPrice = parseFloat((stock.currentPrice * (1 + volatility)).toFixed(2));

      const dailyHigh = Math.max(stock.dailyHigh, newPrice);
      const dailyLow = Math.min(stock.dailyLow, newPrice);

      const direction: 'up' | 'down' | 'neutral' = 
        newPrice > stock.currentPrice ? 'up' : 
        newPrice < stock.currentPrice ? 'down' : 'neutral';

      return {
        ...stock,
        currentPrice: newPrice,
        dailyHigh,
        dailyLow,
        direction,
        lastUpdated: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
      };
    });

    this.stocks = updated;
    this.stocksSubject.next([...this.stocks]);
  }

  toggleStock(ticker: string): void {
    const stock = this.stocks.find(s => s.ticker === ticker);
    if (!stock) return;

    stock.isActive = !stock.isActive;
    if (!stock.isActive) stock.direction = 'neutral';
    
    this.stocksSubject.next([...this.stocks]);
  }

  connectWebSocket(url = 'ws://localhost:8080'): void {
    if (this.ws) this.ws.close();
    
    this.ws = new WebSocket(url);
    
    this.ws.onopen = () => console.log('✅ Connected to real WebSocket server');
    
    this.ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'update') {
        const index = this.stocks.findIndex(s => s.ticker === data.ticker);
        if (index !== -1 && this.stocks[index].isActive) {
          const oldPrice = this.stocks[index].currentPrice;
          const direction = data.price > oldPrice ? 'up' : data.price < oldPrice ? 'down' : 'neutral';
          
          this.stocks[index] = {
            ...this.stocks[index],
            currentPrice: data.price,
            dailyHigh: Math.max(this.stocks[index].dailyHigh, data.price),
            dailyLow: Math.min(this.stocks[index].dailyLow, data.price),
            direction,
            lastUpdated: data.time
          };
          this.stocksSubject.next([...this.stocks]);
        }
      }
    };
  }

  disconnectWebSocket(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
}