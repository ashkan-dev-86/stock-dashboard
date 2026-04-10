export interface IStock {
  symbol: string;
  name: string;
  currentPrice: number;
  dailyHigh: number;
  dailyLow: number;
  fiftyTwoWeekHigh: number;
  fiftyTwoWeekLow: number;
  isActive: boolean;
  direction: 'up' | 'down' | 'neutral';
  lastUpdated: string;
}