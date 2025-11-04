#!/usr/bin/env python3
"""
Distributed E-Voting Data Management System
Using Apache Cassandra for scalable, distributed data storage
"""

from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
import datetime
import logging
from functools import wraps
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra Configuration
CASS_HOSTS = os.getenv('CASSANDRA_HOSTS', '127.0.0.1').split(',')
CASS_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'evoting_db')
CASS_USERNAME = os.getenv('CASSANDRA_USERNAME', 'cassandra')
CASS_PASSWORD = os.getenv('CASSANDRA_PASSWORD', 'cassandra')

# Global Cassandra session
session = None

def init_cassandra():
    """
    Initialize Cassandra connection and create keyspace if needed
    """
    global session
    try:
        auth_provider = PlainTextAuthProvider(username=CASS_USERNAME, password=CASS_PASSWORD)
        cluster = Cluster(CASS_HOSTS, auth_provider=auth_provider)
        session = cluster.connect()
        
        # Create keyspace if it doesn't exist
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASS_KEYSPACE}
            WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 3}}
        """)
        
        session.set_keyspace(CASS_KEYSPACE)
        
        # Create tables
        create_tables()
        logger.info("Cassandra connection established")
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
        raise

def create_tables():
    """
    Create necessary tables in Cassandra
    """
    try:
        # Voters table
        session.execute("""
            CREATE TABLE IF NOT EXISTS voters (
                voter_id UUID PRIMARY KEY,
                voter_name TEXT,
                email TEXT,
                phone TEXT,
                has_voted BOOLEAN,
                voted_at TIMESTAMP,
                created_at TIMESTAMP
            )
        """)
        
        # Votes table
        session.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                vote_id UUID PRIMARY KEY,
                voter_id UUID,
                candidate_id UUID,
                voted_at TIMESTAMP,
                vote_timestamp TIMESTAMP
            )
        """)
        
        # Candidates table
        session.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                candidate_id UUID PRIMARY KEY,
                candidate_name TEXT,
                party TEXT,
                symbol TEXT,
                description TEXT,
                created_at TIMESTAMP
            )
        """)
        
        # Voting Statistics table
        session.execute("""
            CREATE TABLE IF NOT EXISTS voting_stats (
                stat_id UUID PRIMARY KEY,
                total_votes INT,
                total_voters INT,
                votes_by_candidate MAP<UUID, INT>,
                updated_at TIMESTAMP
            )
        """)
        
        logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")

@app.route('/', methods=['GET'])
def home():
    """Serve the main index.html page"""
    return render_template('index.html')

@app.route('/api/voters', methods=['POST'])
def register_voter():
    """Register a new voter"""
    try:
        data = request.get_json()
        voter_id = uuid.uuid4()
        
        query = """
            INSERT INTO voters (voter_id, voter_name, email, phone, has_voted, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        session.execute(query, (
            voter_id,
            data.get('voter_name'),
            data.get('email'),
            data.get('phone'),
            False,
            datetime.datetime.now()
        ))
        
        return jsonify({'success': True, 'voter_id': str(voter_id)}), 201
    except Exception as e:
        logger.error(f"Error registering voter: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/vote', methods=['POST'])
def cast_vote():
    """Record a vote for a candidate"""
    try:
        data = request.get_json()
        voter_id = uuid.UUID(data.get('voter_id'))
        candidate_id = uuid.UUID(data.get('candidate_id'))
        vote_id = uuid.uuid4()
        
        # Check if voter has already voted
        voter_query = "SELECT has_voted FROM voters WHERE voter_id = %s"
        result = session.execute(voter_query, [voter_id])
        
        if result and result[0].has_voted:
            return jsonify({'success': False, 'error': 'Voter has already voted'}), 400
        
        # Record the vote
        vote_query = """
            INSERT INTO votes (vote_id, voter_id, candidate_id, voted_at, vote_timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        session.execute(vote_query, (
            vote_id,
            voter_id,
            candidate_id,
            datetime.datetime.now(),
            datetime.datetime.now()
        ))
        
        # Update voter status
        update_query = "UPDATE voters SET has_voted = true, voted_at = %s WHERE voter_id = %s"
        session.execute(update_query, [datetime.datetime.now(), voter_id])
        
        return jsonify({'success': True, 'vote_id': str(vote_id)}), 201
    except Exception as e:
        logger.error(f"Error casting vote: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/candidates', methods=['GET'])
def get_candidates():
    """Get all candidates"""
    try:
        query = "SELECT * FROM candidates"
        results = session.execute(query)
        candidates = []
        
        for row in results:
            candidates.append({
                'candidate_id': str(row.candidate_id),
                'candidate_name': row.candidate_name,
                'party': row.party,
                'symbol': row.symbol
            })
        
        return jsonify({'candidates': candidates}), 200
    except Exception as e:
        logger.error(f"Error fetching candidates: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    """Get voting statistics"""
    try:
        total_votes_query = "SELECT COUNT(*) FROM votes"
        total_voters_query = "SELECT COUNT(*) FROM voters"
        
        total_votes = session.execute(total_votes_query)[0]
        total_voters = session.execute(total_voters_query)[0]
        
        return jsonify({
            'total_votes': total_votes[0],
            'total_voters': total_voters[0]
        }), 200
    except Exception as e:
        logger.error(f"Error fetching statistics: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'E-Voting Data Management System'}), 200

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    # Initialize Cassandra
    init_cassandra()
    
    # Run Flask app
    debug_mode = os.getenv('FLASK_DEBUG', 'False') == 'True'
    app.run(host='0.0.0.0', port=5000, debug=debug_mode)
