import streamlit as st
import requests
from bs4 import BeautifulSoup
import time
import threading
import queue
import re
import urllib.robotparser
from urllib.parse import urlparse, urljoin
import sqlite3
from datetime import datetime
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import string

nltk.download('punkt_tab')
nltk.download('stopwords')

# Function to extract keywords from the query
def extract_keywords(query):
    stop_words = set(stopwords.words('english'))  # Get common stop words
    words = word_tokenize(query)  # Tokenize the query
    keywords = [word.lower() for word in words if word.isalnum() and word.lower() not in stop_words]
    return keywords  # Returns a list of keywords


# Initialize database with better thread handling
def init_db():
    # Use a connection timeout to prevent locking issues
    conn = sqlite3.connect('search_engine.db', check_same_thread=False, timeout=30.0)
    # Enable WAL mode to improve concurrency
    conn.execute('PRAGMA journal_mode=WAL')
    c = conn.cursor()
    c.execute('''
    CREATE TABLE IF NOT EXISTS pages (
        url TEXT PRIMARY KEY,
        title TEXT,
        content TEXT,
        indexed_at TIMESTAMP,
        last_crawled TIMESTAMP
    )
    ''')
    c.execute('''
    CREATE TABLE IF NOT EXISTS crawl_queue (
        url TEXT PRIMARY KEY,
        priority INTEGER,
        added_at TIMESTAMP
    )
    ''')
    conn.commit()
    return conn


# Crawler class
class RespectfulCrawler:
    def __init__(self, db_connection):
        self.conn = db_connection
        self.crawl_queue = queue.PriorityQueue()
        self.visited_urls = set()
        self.robot_parsers = {}
        self.domain_last_access = {}
        self.crawl_delay = 5  # Default crawl delay in seconds
        self.running = False
        self.lock = threading.Lock()

    def get_robot_parser(self, url):
        domain = urlparse(url).netloc
        if domain not in self.robot_parsers:
            rp = urllib.robotparser.RobotFileParser()
            robots_url = f"https://{domain}/robots.txt"
            try:
                rp.set_url(robots_url)
                rp.read()
                self.robot_parsers[domain] = rp
            except Exception as e:
                self.robot_parsers[domain] = None
        return self.robot_parsers[domain]

    def can_fetch(self, url):
        rp = self.get_robot_parser(url)
        if rp:
            return rp.can_fetch("*", url)
        return True  # If we can't parse robots.txt, we assume permission

    def get_crawl_delay(self, url):
        domain = urlparse(url).netloc
        rp = self.get_robot_parser(url)
        if rp:
            delay = rp.crawl_delay("*")
            if delay:
                return delay
        return self.crawl_delay

    def add_url_to_queue(self, url, priority=1):
        if not url or url in self.visited_urls:
            return

        with self.lock:
            try:
                c = self.conn.cursor()
                c.execute("INSERT OR IGNORE INTO crawl_queue VALUES (?, ?, ?)",
                          (url, priority, datetime.now()))
                self.conn.commit()
                self.crawl_queue.put((priority, url))
            except sqlite3.OperationalError as e:
                # If database is locked, wait and retry
                time.sleep(1)
                self.add_url_to_queue(url, priority)
            except Exception as e:
                pass

    def extract_links(self, soup, base_url):
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(base_url, href)
            # Filter out non-http(s) URLs and fragments
            if full_url.startswith(('http://', 'https://')) and '#' not in full_url:
                links.append(full_url)
        return links

    def crawl_page(self, url):
        domain = urlparse(url).netloc

        # Respect crawl delays
        current_time = time.time()
        if domain in self.domain_last_access:
            time_passed = current_time - self.domain_last_access[domain]
            delay_needed = self.get_crawl_delay(url)
            if time_passed < delay_needed:
                time.sleep(delay_needed - time_passed)

        # Update last access time
        self.domain_last_access[domain] = time.time()

        # Check robots.txt
        if not self.can_fetch(url):
            return []

        try:
            headers = {
                'User-Agent': 'StreatmlitSearchBot/1.0 (respectful research crawler)'
            }
            response = requests.get(url, headers=headers, timeout=10)
            self.domain_last_access[domain] = time.time()

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                # Extract title and text content
                title = soup.title.string if soup.title else url

                # Extract text and remove extra whitespace
                text_content = ' '.join(
                    [p.get_text() for p in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])])
                text_content = re.sub(r'\s+', ' ', text_content).strip()

                # Store in database with retry logic
                retry_count = 0
                while retry_count < 3:
                    with self.lock:
                        try:
                            c = self.conn.cursor()
                            c.execute('''
                            INSERT OR REPLACE INTO pages VALUES (?, ?, ?, ?, ?)
                            ''', (url, title, text_content, datetime.now(), datetime.now()))
                            self.conn.commit()
                            break
                        except sqlite3.OperationalError:
                            # If database is locked, wait and retry
                            retry_count += 1
                            time.sleep(1)
                        except Exception as e:
                            break

                # Extract links for further crawling
                links = self.extract_links(soup, url)
                return links

        except Exception as e:
            pass

        return []

    def start_crawling(self):
        self.running = True
        threading.Thread(target=self.crawl_worker, daemon=True).start()

    def stop_crawling(self):
        self.running = False

    def crawl_worker(self):
        while self.running:
            try:
                # Check database for URLs with retry logic
                retry_count = 0
                url = None
                priority = None

                while retry_count < 3:
                    try:
                        with self.lock:
                            c = self.conn.cursor()
                            c.execute("SELECT url, priority FROM crawl_queue ORDER BY priority, added_at LIMIT 1")
                            result = c.fetchone()

                            if result:
                                url, priority = result
                                c.execute("DELETE FROM crawl_queue WHERE url = ?", (url,))
                                self.conn.commit()
                        break
                    except sqlite3.OperationalError:
                        # If database is locked, wait and retry
                        retry_count += 1
                        time.sleep(1)
                    except Exception as e:
                        break

                if not url:
                    # If queue is empty in DB, wait and try again
                    time.sleep(1)
                    continue

                if url not in self.visited_urls:
                    self.visited_urls.add(url)
                    links = self.crawl_page(url)

                    # Add new links to the queue with lower priority
                    for link in links:
                        if link not in self.visited_urls:
                            self.add_url_to_queue(link, priority + 1)

                # Short pause between operations
                time.sleep(0.1)

            except Exception as e:
                time.sleep(5)  # Wait a bit longer if there's an error


# Function to get favicon for a domain
def get_favicon(url):
    try:
        domain = urlparse(url).netloc
        favicon_url = f"https://www.google.com/s2/favicons?domain={domain}&sz=32"
        return favicon_url
    except:
        # Default favicon if we can't get one
        return "https://cdn.pixabay.com/photo/2014/04/02/17/03/globe-307805_640.png"


# Fixed simple search functionality that will definitely work
# Function to improve search by using keywords
def simple_search(query, db_conn, limit=20):
    keywords = extract_keywords(query)

    if not keywords:
        return []

    # Construct search pattern with extracted keywords
    search_patterns = [f"%{kw}%" for kw in keywords]

    retry_count = 0

    while retry_count < 3:
        try:
            c = db_conn.cursor()

            # Dynamic SQL query using multiple keywords
            query_conditions = " OR ".join(["title LIKE ? OR content LIKE ? OR url LIKE ?"] * len(keywords))
            query_params = [param for kw in search_patterns for param in
                            (kw, kw, kw)]  # Duplicate for title, content, URL

            c.execute(f'''
            SELECT url, title, content 
            FROM pages 
            WHERE {query_conditions}
            ''', query_params)

            results = c.fetchall()

            # Rank results by keyword occurrence
            def rank_result(result):
                url, title, content = result
                title = title or ""
                content = content or ""
                url = url or ""

                score = sum(
                    title.lower().count(kw) * 3 +
                    url.lower().count(kw) * 2 +
                    content.lower().count(kw)
                    for kw in keywords
                )
                return score

            sorted_results = sorted(results, key=rank_result, reverse=True)[:limit]
            return sorted_results

        except sqlite3.OperationalError:
            retry_count += 1
            time.sleep(1)
        except Exception as e:
            st.error(f"Search error: {str(e)}")
            break

    return []




# Function to highlight search terms in text
def highlight_terms(text, query):
    if not text or not query:
        return text

    highlighted = text
    try:
        pattern = re.compile(re.escape(query), re.IGNORECASE)
        highlighted = pattern.sub(f'<span class="highlight">{query}</span>', highlighted)
    except:
        pass

    return highlighted


# Initialize the app
st.set_page_config(
    page_title="Dove - Search Engine",
    page_icon="🕊️",
    layout="wide",
)

# Apply custom styling with dark mode
st.markdown("""
<style>
    /* Global dark theme for Streamlit */
    [data-testid="stAppViewContainer"] {
        background-color: #121212;
        color: #e0e0e0;
    }

    /* Header style */
    .main-header {
        margin-bottom: 20px;
        color: #f0f0f0;
    }

    /* Search Result Styles */
    .search-result {
        margin-bottom: 20px;
        padding: 15px;
        border-radius: 5px;
        background-color: #1e1e1e;
        border-left: 3px solid #8c52ff;
        transition: all 0.2s ease;
        display: flex;
        align-items: flex-start;
    }

    .search-result:hover {
        transform: translateX(5px);
        border-left: 3px solid #a876ff;
    }

    .favicon {
        margin-right: 10px;
        width: 16px;
        height: 16px;
        flex-shrink: 0;
    }

    .result-content {
        flex-grow: 1;
    }

    .search-title {
        font-size: 1.2em;
        color: #a876ff;
        margin-bottom: 5px;
        font-weight: bold;
    }

    .search-url {
        color: #8c9eff;
        font-size: 0.8em;
        margin-bottom: 8px;
        word-break: break-all;
    }

    .search-snippet {
        color: #b0b0b0;
        line-height: 1.5;
        font-size: 0.95em;
    }

    .highlight {
        background-color: rgba(140, 82, 255, 0.3);
        color: #ffffff;
        padding: 0 2px;
        border-radius: 3px;
    }

    /* Search stats */
    .search-stats {
        font-size: 0.9em;
        color: #909090;
        margin-bottom: 20px;
    }

    /* Input field */
    .stTextInput > div > div > input {
        color: #f0f0f0;
        background-color: #2a2a2a;
        border: 1px solid #444444;
    }

    /* Button styling */
    .stButton > button {
        width: 100%;
        background-color: #8c52ff;
        color: white;
        border: none;
    }

    .stButton > button:hover {
        background-color: #a876ff;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #1a1a1a;
    }

    [data-testid="stSidebar"] > div:first-child {
        background-color: #1a1a1a;
    }

    /* Metrics styling */
    [data-testid="stMetricValue"] {
        color: #a876ff;
    }

    /* Recent pages */
    .recent-page {
        padding: 10px;
        margin-bottom: 5px;
        background-color: #222222;
        border-radius: 3px;
    }

    /* No results message */
    .no-results {
        text-align: center;
        padding: 30px;
        color: #909090;
        background-color: #1e1e1e;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# Initialize the database connection
conn = init_db()

# Initialize the crawler
crawler = RespectfulCrawler(conn)


# Make sure we have some data in the database
def ensure_data():
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM pages")
    count = c.fetchone()[0]

    if count == 0:
        # Add some sample data if database is empty
        sample_data = [
            (
                "https://moz.com/top500",
                "Top 500 Most Popular Websites - Moz",
                "Moz's list of the most popular 500 websites on the internet, based on an index of over 40 trillion links!"
            )
        ]

        for url, title, content in sample_data:
            c.execute(
                "INSERT OR IGNORE INTO pages VALUES (?, ?, ?, ?, ?)",
                (url, title, content, datetime.now(), datetime.now())
            )

        conn.commit()


# Make sure we have data
ensure_data()

# App title
st.markdown('<h1 class="main-header">🕊️ Dove Search</h1>', unsafe_allow_html=True)

# Sidebar with crawler controls
with st.sidebar:
    st.header("Crawler Controls")

    # Add a website to the FRONT of the crawler queue
    high_priority_url = st.text_input("Add a website (High Priority):", "")
    if st.button("Refresh"):
            st.rerun()
    if st.button("Add to Front of Queue"):
        if high_priority_url:
            crawler.add_url_to_queue(high_priority_url, priority=0)  # Highest priority
            st.success(f"Added {high_priority_url} to the front of the queue.")

    # Normal URL addition (admin mode)
    if 'admin_mode' not in st.session_state:
        st.session_state.admin_mode = False

    if st.session_state.admin_mode:
        seed_url = st.text_input("Add a seed URL to crawl:", "")

        if st.button("Add URL to Crawler Queue"):
            if seed_url:
                crawler.add_url_to_queue(seed_url, priority=1)  # Normal priority
                st.success(f"Added {seed_url} to crawler queue")

    # Crawler status and controls
    crawler_status = st.empty()

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Start Crawler"):
            crawler.start_crawling()
            st.session_state.crawler_running = True

    with col2:
        if st.button("Stop Crawler"):
            crawler.stop_crawling()
            st.session_state.crawler_running = False

    # Show crawler stats
    st.subheader("Crawler Stats")

    # Count pages in the database with retry logic
    retry_count = 0
    page_count = 0
    queue_count = 0

    while retry_count < 3:
        try:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM pages")
            page_count = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM crawl_queue")
            queue_count = c.fetchone()[0]
            break
        except sqlite3.OperationalError:
            # If database is locked, wait and retry
            retry_count += 1
            time.sleep(1)
        except Exception as e:
            break

    st.metric("Pages Indexed", page_count)
    st.metric("URLs in Queue", queue_count)

    # Update crawler status
    if 'crawler_running' not in st.session_state:
        st.session_state.crawler_running = False

    status = "Running" if st.session_state.crawler_running else "Stopped"
    crawler_status.success(f"Crawler Status: {status}")

    # Display the last 5 crawled pages
    st.subheader("Recently Indexed Pages")

    retry_count = 0
    recent_pages = []

    while retry_count < 3:
        try:
            c = conn.cursor()
            c.execute("SELECT url, last_crawled FROM pages ORDER BY last_crawled DESC LIMIT 5")
            recent_pages = c.fetchall()
            break
        except sqlite3.OperationalError:
            # If database is locked, wait and retry
            retry_count += 1
            time.sleep(1)
        except Exception as e:
            break

    for url, timestamp in recent_pages:
        st.markdown(f'<div class="recent-page"><b>{url}</b><br/><small>Indexed: {timestamp}</small></div>',
                    unsafe_allow_html=True)

# Main search interface
search_query = st.text_input("Search for:", "")

if search_query:
    # Get search results with simple search to ensure it works
    results = simple_search(search_query, conn)

    # Display search stats
    st.markdown(f'<div class="search-stats">Found {len(results)} results for "{search_query}"</div>',
                unsafe_allow_html=True)

    if not results:
        st.markdown(
            '<div class="no-results"><h3>No results found</h3><p>The crawler may still be building the index or no matching content was found.</p><br><img src="https://cdn-icons-png.flaticon.com/512/6134/6134065.png" alt=""></div>',
            unsafe_allow_html=True)

    # Display results with favicons
    for url, title, content in results:
        # Get favicon for the URL
        favicon_url = get_favicon(url)

        # Ensure content is not None and highlight search terms
        content_snippet = content[:300] + "..." if content and len(content) > 300 else content or ""

        # Highlight search terms
        highlighted_title = highlight_terms(title or url, search_query)
        highlighted_snippet = highlight_terms(content_snippet, search_query)

        st.markdown(f"""
        <a href="{url}" target="_blank" style="text-decoration: none; color: inherit;">
            <div class="search-result">
                <img src="{favicon_url}" class="favicon" alt="favicon">
                <div class="result-content">
                    <div class="search-title">{highlighted_title}</div>
                    <div class="search-url">{url}</div>
                    <div class="search-snippet">{highlighted_snippet}</div>
                </div>
            </div>
        </a>
        """, unsafe_allow_html=True)

# Start the crawler automatically if it's the first run
if 'first_run' not in st.session_state:
    st.session_state.first_run = True
    # Add default seed URLs
    default_seeds = [
        "https://moz.com/top500",
        "https://en.wikipedia.org/wiki/Web_crawler",
        "https://en.wikipedia.org/wiki/Information_retrieval",
        "https://en.wikipedia.org/wiki/Web_search_engine",
        "https://en.wikipedia.org/wiki/PageRank",
        "https://en.wikipedia.org/wiki/Google",
    ]
    for seed in default_seeds:
        crawler.add_url_to_queue(seed)
    crawler.start_crawling()
    st.session_state.crawler_running = True

