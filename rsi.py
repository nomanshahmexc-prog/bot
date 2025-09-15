import pandas as pd
import numpy as np
import aiohttp
import asyncio
import time
import warnings
from datetime import datetime
import requests
from typing import List
import threading
from collections import deque
import sys
import traceback

warnings.filterwarnings('ignore')

# ================== PUSHBULLET TOKEN ==================
PUSHBULLET_TOKEN = "o.SJ5wXkGzsBaU9W1kyMqLsIz8kEYJXP4Z"

# ================== NOTIFICATION MANAGER ==================
class NotificationManager:
    def __init__(self, token, min_interval=2):
        self.token = token
        self.queue = deque()
        self.failed_notifications = []
        self.last_sent_time = 0
        self.min_interval = min_interval
        self.lock = threading.Lock()
        self.cooldowns = {}  # symbol: last_sent_timestamp
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()

    def _send_immediate(self, title, body):
        try:
            data_send = {"type": "note", "title": title, "body": body}
            response = requests.post(
                "https://api.pushbullet.com/v2/pushes",
                json=data_send,
                headers={
                    "Access-Token": self.token,
                    "Content-Type": "application/json"
                },
                timeout=5
            )
            if response.status_code == 200:
                print(f"📲 Pushbullet Notification Sent: {title}")
                return True
            else:
                print(f"⚠️ Pushbullet Error: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"❌ Pushbullet Exception: {e}")
            return False

    def _worker(self):
        while True:
            if self.queue:
                with self.lock:
                    if self.queue:  # Double check after acquiring lock
                        notif = self.queue.popleft()
                    else:
                        time.sleep(0.5)
                        continue
                        
                now = time.time()
                # enforce min_interval gap
                if now - self.last_sent_time < self.min_interval:
                    time.sleep(self.min_interval - (now - self.last_sent_time))
                ok = self._send_immediate(notif["title"], notif["body"])
                if not ok:
                    notif["retries"] += 1
                    if notif["retries"] <= 3:
                        print(f"🔄 Retrying {notif['title']} (Attempt {notif['retries']})")
                        with self.lock:
                            self.queue.append(notif)
                    else:
                        self.failed_notifications.append(notif)
                else:
                    self.last_sent_time = time.time()
            else:
                time.sleep(0.5)

    def send(self, title, body, symbol):
        now = time.time()

        # cooldown per symbol (3 min)
        if symbol in self.cooldowns and now - self.cooldowns[symbol] < 180:
            print(f"⏳ Cooldown active for {symbol}, skipping {title}")
            return

        # duplicate filter (same title in <60 sec)
        for notif in list(self.queue):
            if notif['title'] == title and (now - notif['timestamp']) < 60:
                print(f"🔄 Duplicate notification skipped: {title}")
                return

        notif = {
            "title": title,
            "body": body,
            "timestamp": now,
            "symbol": symbol,
            "retries": 0
        }

        with self.lock:
            self.queue.append(notif)

        self.cooldowns[symbol] = now


# ================== GLOBAL NOTIFICATION MANAGER ==================
notifier = NotificationManager(PUSHBULLET_TOKEN)


# ================== RSI CLASS ==================
class RSIIndicator:
    def __init__(self, length=14):
        self.length = length

    def rma(self, series, length):
        try:
            alpha = 1.0 / length
            return series.ewm(alpha=alpha, adjust=False).mean()
        except Exception as e:
            print(f"❌ RMA calculation error: {e}")
            return pd.Series([50.0] * len(series), index=series.index)

    def calculate_rsi(self, close_prices):
        try:
            if len(close_prices) < self.length + 1:
                return pd.Series([50.0] * len(close_prices), index=close_prices.index)

            changes = close_prices.diff()
            gains = changes.where(changes > 0, 0.0)
            losses = (-changes).where(changes < 0, 0.0)

            avg_gains = self.rma(gains, self.length)
            avg_losses = self.rma(losses, self.length)

            # Handle division by zero
            avg_losses = avg_losses.replace(0, 0.0001)
            rs = avg_gains / avg_losses
            rsi = 100 - (100 / (1 + rs))

            return rsi.fillna(50)
        except Exception as e:
            print(f"❌ RSI calculation error: {e}")
            return pd.Series([50.0] * len(close_prices), index=close_prices.index)


# ================== SUPPORT/RESISTANCE ==================
class SupportResistanceDetector:
    def __init__(self, lookback_period=20):
        self.lookback_period = lookback_period

    def find_support_resistance(self, df):
        try:
            if len(df) < self.lookback_period:
                return None

            recent_data = df.tail(self.lookback_period).copy()

            support_idx = recent_data['low'].idxmin()
            support_level = recent_data.loc[support_idx, 'low']

            resistance_idx = recent_data['high'].idxmax()
            resistance_level = recent_data.loc[resistance_idx, 'high']

            return {
                'support_level': float(support_level),
                'resistance_level': float(resistance_level)
            }
        except Exception as e:
            print(f"❌ S/R calculation error: {e}")
            return None


# ================== GET TOP PERPETUALS ==================
def get_top_n_perpetuals(n: int = 50, quote_filter: str = "USDT") -> List[str]:
    try:
        print(f"🔍 Fetching top {n} perpetuals...")
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        filtered = [
            item for item in data
            if item.get('symbol', '').endswith(quote_filter)
            and ('DOWN' not in item['symbol'] and 'UP' not in item['symbol'])
            and item.get('symbol') != 'USDCUSDT'  # Exclude stablecoin pairs
        ]

        for item in filtered:
            try:
                item['quoteVolume'] = float(item.get('quoteVolume', 0) or 0)
            except:
                item['quoteVolume'] = 0.0

        filtered_sorted = sorted(filtered, key=lambda x: x['quoteVolume'], reverse=True)
        top_symbols = [item['symbol'] for item in filtered_sorted[:n]]

        print(f"✅ Fetched {len(top_symbols)} top perpetual symbols from Binance")
        if len(top_symbols) > 0:
            print(f"📈 Top 5: {top_symbols[:5]}")
        return top_symbols

    except Exception as e:
        print(f"⚠️ Error fetching top perpetuals: {e}")
        print(f"🔄 Using fallback symbols...")
        return ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'AVAXUSDT']


# ================== SCANNER ==================
class UltraFastRSIScanner:
    def __init__(self, symbols: List[str] = None):
        self.base_url = "https://fapi.binance.com/fapi/v1/klines"

        if symbols is None:
            symbols = get_top_n_perpetuals(50)  # Reduced from 100 to 50 for better performance
            if not symbols:
                symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT']
                print("⚠️ Using fallback symbols")

        self.symbols = symbols
        print(f"🎯 Scanning {len(self.symbols)} symbols")

        self.rsi_1m = RSIIndicator(length=14)
        self.rsi_15m = RSIIndicator(length=14)
        self.sr_detector = SupportResistanceDetector(lookback_period=20)

        self.data_1m = {}
        self.data_15m = {}
        self.last_signals = {symbol: {'type': None, 'time': None, 'candle_index': None} for symbol in self.symbols}
        self.active_signals = []

        for symbol in self.symbols:
            self.data_1m[symbol] = pd.DataFrame()
            self.data_15m[symbol] = pd.DataFrame()

    async def fetch_klines_direct(self, session, symbol, interval, limit=100):
        try:
            url = f"{self.base_url}?symbol={symbol}&interval={interval}&limit={limit}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    if not data:
                        return pd.DataFrame()
                        
                    df = pd.DataFrame(data, columns=[
                        'timestamp', 'open', 'high', 'low', 'close', 'volume',
                        'close_time', 'quote_volume', 'count', 'taker_buy_base',
                        'taker_buy_quote', 'ignore'
                    ])
                    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                    
                    # More robust data type conversion
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Remove any rows with NaN values
                    df = df.dropna()
                    
                    if df.empty:
                        return pd.DataFrame()
                        
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    df.set_index('timestamp', inplace=True)
                    return df
                else:
                    print(f"⚠️ HTTP {response.status} for {symbol} {interval}")
                    return pd.DataFrame()
        except Exception as e:
            print(f"❌ Error fetching {symbol} {interval}: {str(e)}")
            return pd.DataFrame()

    async def initialize_data(self):
        print("📊 Loading initial data...")
        connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=300, ttl_dns_cache_ttl=600)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            for symbol in self.symbols:
                tasks.append(self.fetch_klines_direct(session, symbol, '1m', 100))
                tasks.append(self.fetch_klines_direct(session, symbol, '15m', 100))
            
            print(f"🚀 Starting {len(tasks)} data fetch tasks...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful_symbols = []
            for i, symbol in enumerate(self.symbols):
                try:
                    result_1m = results[i * 2]
                    result_15m = results[i * 2 + 1]
                    
                    if isinstance(result_1m, pd.DataFrame) and not result_1m.empty:
                        self.data_1m[symbol] = result_1m
                        print(f"✅ {symbol} 1m: {len(result_1m)} candles")
                        successful_symbols.append(symbol)
                    else:
                        print(f"❌ Failed to get 1m data for {symbol}")
                        
                    if isinstance(result_15m, pd.DataFrame) and not result_15m.empty:
                        self.data_15m[symbol] = result_15m
                        print(f"✅ {symbol} 15m: {len(result_15m)} candles")
                    else:
                        print(f"❌ Failed to get 15m data for {symbol}")
                        
                except Exception as e:
                    print(f"❌ Error processing {symbol}: {str(e)}")
                    
        print(f"📊 Successfully loaded data for {len(successful_symbols)} symbols")

    async def update_all_data(self):
        connector = aiohttp.TCPConnector(limit=30)
        timeout = aiohttp.ClientTimeout(total=15)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            for symbol in self.symbols:
                if symbol in self.data_1m and symbol in self.data_15m:
                    tasks.append(self.fetch_klines_direct(session, symbol, '1m', 2))
                    tasks.append(self.fetch_klines_direct(session, symbol, '15m', 2))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            task_index = 0
            for symbol in self.symbols:
                if symbol not in self.data_1m or symbol not in self.data_15m:
                    continue
                    
                try:
                    result_1m = results[task_index]
                    result_15m = results[task_index + 1]
                    task_index += 2
                    
                    if isinstance(result_1m, pd.DataFrame) and not result_1m.empty:
                        latest_1m = result_1m.iloc[-1:]
                        if len(self.data_1m[symbol]) > 0:
                            last_timestamp = self.data_1m[symbol].index[-1]
                            new_timestamp = latest_1m.index[0]
                            if new_timestamp == last_timestamp:
                                self.data_1m[symbol].iloc[-1] = latest_1m.iloc[0]
                            elif new_timestamp > last_timestamp:
                                self.data_1m[symbol] = pd.concat([self.data_1m[symbol], latest_1m])
                                if len(self.data_1m[symbol]) > 150:
                                    self.data_1m[symbol] = self.data_1m[symbol].tail(150)
                    
                    if isinstance(result_15m, pd.DataFrame) and not result_15m.empty:
                        latest_15m = result_15m.iloc[-1:]
                        if len(self.data_15m[symbol]) > 0:
                            last_timestamp = self.data_15m[symbol].index[-1]
                            new_timestamp = latest_15m.index[0]
                            if new_timestamp == last_timestamp:
                                self.data_15m[symbol].iloc[-1] = latest_15m.iloc[0]
                            elif new_timestamp > last_timestamp:
                                self.data_15m[symbol] = pd.concat([self.data_15m[symbol], latest_15m])
                                if len(self.data_15m[symbol]) > 150:
                                    self.data_15m[symbol] = self.data_15m[symbol].tail(150)
                except Exception as e:
                    print(f"❌ Update error for {symbol}: {str(e)}")
                    continue

    def analyze_symbol(self, symbol):
        try:
            df_1m = self.data_1m.get(symbol)
            df_15m = self.data_15m.get(symbol)
            
            if df_1m is None or df_15m is None:
                return None
                
            if len(df_1m) < 30 or len(df_15m) < 30:
                return None

            # Calculate RSI with error handling
            rsi_1m_values = self.rsi_1m.calculate_rsi(df_1m['close'])
            rsi_15m_values = self.rsi_15m.calculate_rsi(df_15m['close'])

            if len(rsi_1m_values) == 0 or len(rsi_15m_values) == 0:
                return None
                
            current_rsi_1m = rsi_1m_values.iloc[-1]
            current_rsi_15m = rsi_15m_values.iloc[-1]

            if pd.isna(current_rsi_1m) or pd.isna(current_rsi_15m):
                return None

            # Calculate S/R levels
            sr_levels = self.sr_detector.find_support_resistance(df_15m)
            if sr_levels is None:
                return None

            current_candle = df_15m.iloc[-1]
            current_price = float(current_candle['close'])

            # Check for support/resistance touches
            support_touch = (float(current_candle['low']) <= sr_levels['support_level'] * 1.01 and
                             current_price > sr_levels['support_level'])
            resistance_touch = (float(current_candle['high']) >= sr_levels['resistance_level'] * 0.99 and
                                current_price < sr_levels['resistance_level'])

            signals = []
            current_time = datetime.now()
            current_candle_index = len(df_15m)
            last_signal = self.last_signals[symbol]

            # Generate signals
            if (support_touch and current_rsi_1m <= 35 and current_rsi_15m <= 35):
                if last_signal['type'] == 'BUY' and last_signal['candle_index'] is not None:
                    if current_candle_index - last_signal['candle_index'] < 10:
                        return {
                            'symbol': symbol, 'price': current_price,
                            'rsi_1m': float(current_rsi_1m), 'rsi_15m': float(current_rsi_15m),
                            'support_level': sr_levels['support_level'], 'resistance_level': sr_levels['resistance_level'],
                            'support_touch': support_touch, 'resistance_touch': resistance_touch,
                            'signals': [], 'last_update': df_1m.index[-1] if not df_1m.empty else None
                        }
                        
                signal = {
                    'symbol': symbol, 'type': 'BUY', 'price': current_price,
                    'rsi_1m': float(current_rsi_1m), 'rsi_15m': float(current_rsi_15m),
                    'support_level': sr_levels['support_level'], 'resistance_level': sr_levels['resistance_level'],
                    'time': current_time
                }
                signals.append(signal)
                self.last_signals[symbol] = {'type': 'BUY', 'time': current_time, 'candle_index': current_candle_index}
                notifier.send("🟢 BUY SIGNAL 🚀", f"{symbol} @ {current_price:.4f}\nRSI1m={current_rsi_1m:.1f}, RSI15m={current_rsi_15m:.1f}", symbol)

            elif (resistance_touch and current_rsi_1m >= 70 and current_rsi_15m >= 70):
                if last_signal['type'] == 'SELL' and last_signal['candle_index'] is not None:
                    if current_candle_index - last_signal['candle_index'] < 10:
                        return {
                            'symbol': symbol, 'price': current_price,
                            'rsi_1m': float(current_rsi_1m), 'rsi_15m': float(current_rsi_15m),
                            'support_level': sr_levels['support_level'], 'resistance_level': sr_levels['resistance_level'],
                            'support_touch': support_touch, 'resistance_touch': resistance_touch,
                            'signals': [], 'last_update': df_1m.index[-1] if not df_1m.empty else None
                        }
                        
                signal = {
                    'symbol': symbol, 'type': 'SELL', 'price': current_price,
                    'rsi_1m': float(current_rsi_1m), 'rsi_15m': float(current_rsi_15m),
                    'support_level': sr_levels['support_level'], 'resistance_level': sr_levels['resistance_level'],
                    'time': current_time
                }
                signals.append(signal)
                self.last_signals[symbol] = {'type': 'SELL', 'time': current_time, 'candle_index': current_candle_index}
                notifier.send("🔴 SELL SIGNAL ❌", f"{symbol} @ {current_price:.4f}\nRSI1m={current_rsi_1m:.1f}, RSI15m={current_rsi_15m:.1f}", symbol)

            return {
                'symbol': symbol, 'price': current_price,
                'rsi_1m': float(current_rsi_1m), 'rsi_15m': float(current_rsi_15m),
                'support_level': sr_levels['support_level'], 'resistance_level': sr_levels['resistance_level'],
                'support_touch': support_touch, 'resistance_touch': resistance_touch,
                'signals': signals, 'last_update': df_1m.index[-1] if not df_1m.empty else None
            }

        except Exception as e:
            print(f"❌ Analysis error for {symbol}: {str(e)}")
            return None

    def scan_all_symbols(self):
        results = []
        signals = []
        successful_scans = 0
        
        for symbol in self.symbols:
            try:
                result = self.analyze_symbol(symbol)
                if result:
                    results.append(result)
                    successful_scans += 1
                    if result['signals']:
                        signals.extend(result['signals'])
            except Exception as e:
                print(f"❌ Scan error for {symbol}: {str(e)}")
                
        print(f"📊 Successfully analyzed {successful_scans}/{len(self.symbols)} symbols")
        return results, signals

    def print_results(self, results, signals, fetch_time):
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Clear screen only on Unix systems
            if sys.platform != 'win32':
                print("\033[2J\033[H", end="")
            else:
                print("\n" + "="*80)
                
            print(f"🚀 ULTRA FAST RSI SCANNER: {current_time}")
            print(f"⚡ Fetch Time: {fetch_time:.2f}s | Analyzed: {len(results)} symbols")
            print("=" * 80)

            print("\n📌 ACTIVE SIGNALS (Last 3 min)")
            print("-" * 80)
            if self.active_signals:
                for sig in self.active_signals[-10:]:  # Show last 10 signals
                    sig_type = "🟢 BUY" if sig['type'] == 'BUY' else "🔴 SELL"
                    print(f"{sig_type} - {sig['symbol']} | Price: ${sig['price']:.4f} | "
                          f"RSI 1m: {sig['rsi_1m']:.1f} | 15m: {sig['rsi_15m']:.1f} | "
                          f"⏰ {sig['time'].strftime('%H:%M:%S')}")
            else:
                print("⚪ No active signals in last 3 minutes.")
            print("=" * 80)

            if signals:
                print("\n🚨 🚨 NEW SIGNALS 🚨 🚨")
                print("-" * 60)
                for signal in signals:
                    signal_type = "🟢 BUY" if signal['type'] == 'BUY' else "🔴 SELL"
                    print(f"{signal_type} - {signal['symbol']}")
                    print(f"   💰 Price: ${signal['price']:.4f}")
                    print(f"   📊 RSI 1m: {signal['rsi_1m']:.1f} | 15m: {signal['rsi_15m']:.1f}")
                    print(f"   📈 S/R: ${signal['support_level']:.4f} / ${signal['resistance_level']:.4f}")
                    print("-" * 60)

            print(f"\n📊 LIVE STATUS (Top 20):")
            print("-" * 80)
            print(f"{'COIN':<10} {'PRICE':<12} {'RSI-1m':<8} {'RSI-15m':<9} {'S/R':<5} {'STATUS':<12}")
            print("-" * 80)
            
            # Sort by signals first, then by symbol name, show top 20
            sorted_results = sorted(results, key=lambda x: (len(x['signals']) > 0, x['symbol']), reverse=True)
            
            for result in sorted_results[:20]:
                try:
                    sup_touch = "🟢" if result.get('support_touch', False) else "⚪"
                    res_touch = "🔴" if result.get('resistance_touch', False) else "⚪"
                    rsi_1m_color = "🔴" if result['rsi_1m'] >= 70 else "🟢" if result['rsi_1m'] <= 30 else "⚪"
                    rsi_15m_color = "🔴" if result['rsi_15m'] >= 70 else "🟢" if result['rsi_15m'] <= 30 else "⚪"
                    status = "⚡SIGNAL!" if result.get('signals') and len(result['signals']) > 0 else "👁️ WATCH"
                    
                    print(f"{result['symbol']:<10} ${result['price']:<11.4f} "
                          f"{result['rsi_1m']:<7.1f}{rsi_1m_color} {result['rsi_15m']:<8.1f}{rsi_15m_color} "
                          f"{sup_touch}{res_touch}  {status:<12}")
                except Exception as e:
                    print(f"❌ Display error for result: {str(e)}")
                    
            print(f"\n💡 BUY: Support Touch + RSI≤35 | SELL: Resistance Touch + RSI≥70")
            print(f"🔄 Next update in 3 seconds...")
            
        except Exception as e:
            print(f"❌ Display error: {str(e)}")


# ================== MAIN LOOP ==================
async def main_loop():
    try:
        print("🚀 Starting Ultra Fast RSI Scanner...")
        scanner = UltraFastRSIScanner()
        
        print("📡 Initializing data...")
        await scanner.initialize_data()
        
        print("🔄 Starting main scanning loop...")
        loop_count = 0
        
        while True:
            try:
                start_time = time.time()
                
                # Update data
                await scanner.update_all_data()
                
                # Scan all symbols
                results, signals = scanner.scan_all_symbols()
                
                # Update active signals
                current_time = datetime.now()
                scanner.active_signals = [
                    sig for sig in scanner.active_signals
                    if (current_time - sig['time']).total_seconds() < 180
                ]
                
                if signals:
                    scanner.active_signals.extend(signals)
                
                # Calculate fetch time
                fetch_time = time.time() - start_time
                
                # Print results
                scanner.print_results(results, signals, fetch_time)
                
                loop_count += 1
                if loop_count % 20 == 0:  # Every 20 loops (1 minute), print status
                    print(f"🔄 Loop #{loop_count} completed. System running normally.")
                
                # Wait before next iteration
                await asyncio.sleep(3)
                
            except Exception as e:
                print(f"❌ Main loop error: {str(e)}")
                print(f"🔄 Continuing in 5 seconds...")
                await asyncio.sleep(5)
                
    except Exception as e:
        print(f"❌ Fatal error in main_loop: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    try:
        print("🎯 Ultra Fast RSI Scanner starting...")
        print("🖥️  Platform:", sys.platform)
        print("🐍 Python version:", sys.version)
        
        # Set up asyncio for different platforms
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        asyncio.run(main_loop())
        
    except KeyboardInterrupt:
        print("\n👋 Scanner stopped by user")
    except Exception as e:
        print(f"❌ Fatal startup error: {str(e)}")
        traceback.print_exc()
