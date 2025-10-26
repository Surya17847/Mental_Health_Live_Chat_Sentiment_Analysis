import re
import json
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime

class MentalHealthSentimentAnalyzer:
    def __init__(self):
        self.vader_analyzer = SentimentIntensityAnalyzer()
        
        # Enhanced mental health keywords with weights
        self.mental_health_keywords = {
            # High severity negative terms
            'suicidal': 2.0, 'kill myself': 2.0, 'end it all': 2.0, 'want to die': 2.0,
            'self harm': 2.0, 'cutting': 2.0, 'overdose': 2.0,
            
            # Medium severity negative terms
            'depressed': 1.5, 'depression': 1.5, 'hopeless': 1.5, 'desperate': 1.5,
            'overwhelmed': 1.5, 'anxious': 1.5, 'anxiety': 1.5, 'panic attack': 1.5,
            'burnout': 1.5, 'exhausted': 1.5, 'trauma': 1.5, 'ptsd': 1.5,
            
            # Low severity negative terms
            'stress': 1.2, 'stressed': 1.2, 'sad': 1.2, 'unhappy': 1.2, 'worried': 1.2,
            'nervous': 1.2, 'scared': 1.2, 'afraid': 1.2,
            
            # General mental health terms
            'therapy': 1.0, 'therapist': 1.0, 'mental health': 1.0, 'counseling': 1.0,
            'psychiatrist': 1.0, 'medication': 1.0, 'bipolar': 1.0, 'ocd': 1.0,
            'adhd': 1.0, 'autism': 1.0, 'eating disorder': 1.0
        }
        
        self.positive_mental_health_keywords = {
            'happy': 1.5, 'better': 1.5, 'improving': 1.5, 'progress': 1.5, 
            'grateful': 1.5, 'thankful': 1.5, 'recovery': 1.5, 'healing': 1.5,
            'wellness': 1.5, 'meditation': 1.2, 'mindfulness': 1.2, 'coping': 1.2,
            'strength': 1.2, 'support': 1.2, 'help': 1.2, 'proud': 1.5,
            'confident': 1.5, 'optimistic': 1.5, 'hopeful': 1.5
        }

    def clean_text(self, text):
        """Clean the text for analysis"""
        # Remove URLs
        text = re.sub(r'http\S+', '', text)
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove special characters but keep basic punctuation and emoticons
        text = re.sub(r'[^\w\s!?.,;:()\-\']', '', text)
        return text.lower().strip()

    def detect_emoticons(self, text):
        """Detect and score emoticons"""
        positive_emoticons = [':)', ':-)', ':D', ':-D', ':]', ':-]', ': )', '=)', '😊', '😄', '😃', '🙂', '🤗']
        negative_emoticons = [':(', ':-(', ':[', ':-[', ':(', '=(', '😔', '😢', '😞', '😟', '🙁', '😥']
        
        positive_count = sum(1 for emoticon in positive_emoticons if emoticon in text)
        negative_count = sum(1 for emoticon in negative_emoticons if emoticon in text)
        
        return positive_count - negative_count

    def contains_mental_health_context(self, text):
        """Check if text contains mental health related terms with weights"""
        text_lower = text.lower()
        
        mental_health_score = 0
        positive_context_score = 0
        
        # Check for negative mental health terms
        for keyword, weight in self.mental_health_keywords.items():
            if keyword in text_lower:
                mental_health_score += weight
        
        # Check for positive mental health terms
        for keyword, weight in self.positive_mental_health_keywords.items():
            if keyword in text_lower:
                positive_context_score += weight
        
        # Emoticon influence
        emoticon_score = self.detect_emoticons(text)
        positive_context_score += max(0, emoticon_score) * 0.5
        mental_health_score += max(0, -emoticon_score) * 0.5
        
        return {
            'is_mental_health': mental_health_score > 0 or positive_context_score > 0,
            'mental_health_score': mental_health_score,
            'positive_context_score': positive_context_score,
            'has_any_context': mental_health_score > 0 or positive_context_score > 0,
            'emoticon_score': emoticon_score
        }

    def analyze_sentiment(self, text):
        """Comprehensive sentiment analysis with enhanced mental health context"""
        if not text or len(text.strip()) < 2:
            return self._default_sentiment()

        cleaned_text = self.clean_text(text)
        
        # VADER sentiment analysis
        vader_scores = self.vader_analyzer.polarity_scores(cleaned_text)
        
        # TextBlob sentiment
        blob = TextBlob(cleaned_text)
        textblob_polarity = blob.sentiment.polarity
        textblob_subjectivity = blob.sentiment.subjectivity
        
        # Mental health context
        context = self.contains_mental_health_context(cleaned_text)
        
        # Enhanced sentiment determination
        compound_score = vader_scores['compound']
        
        # Adjust score based on mental health context
        adjusted_score = self._adjust_sentiment_score(
            compound_score, 
            context, 
            textblob_subjectivity
        )
        
        sentiment_result = self._classify_sentiment(adjusted_score, context, text)
        
        return {
            'text': text,
            'cleaned_text': cleaned_text,
            'sentiment_label': sentiment_result['label'],
            'sentiment_score': adjusted_score,
            'original_score': compound_score,
            'confidence': sentiment_result['confidence'],
            'severity': sentiment_result['severity'],
            'mental_health_context': context,
            'vader_scores': vader_scores,
            'textblob_polarity': textblob_polarity,
            'textblob_subjectivity': textblob_subjectivity,
            'timestamp': datetime.now().isoformat(),
            'needs_attention': sentiment_result['needs_attention'],
            'risk_level': sentiment_result['risk_level'],
            'analysis_notes': sentiment_result.get('notes', [])
        }

    def _adjust_sentiment_score(self, original_score, context, subjectivity):
        """Adjust sentiment score based on context and subjectivity"""
        adjusted_score = original_score
        
        # Mental health context adjustments
        if context['is_mental_health']:
            # Boost positive mental health messages
            if context['positive_context_score'] > context['mental_health_score']:
                adjusted_score += 0.2 * min(context['positive_context_score'], 3)
            # Amplify negative mental health messages
            elif context['mental_health_score'] > context['positive_context_score']:
                adjusted_score -= 0.15 * min(context['mental_health_score'], 4)
        
        # Emoticon adjustments
        adjusted_score += context['emoticon_score'] * 0.1
        
        # Subjectivity adjustments (more subjective = more confident in score)
        confidence_boost = subjectivity * 0.1
        if adjusted_score > 0:
            adjusted_score += confidence_boost
        elif adjusted_score < 0:
            adjusted_score -= confidence_boost
        
        return max(-1.0, min(1.0, adjusted_score))

    def _classify_sentiment(self, score, context, original_text):
        """Enhanced sentiment classification"""
        notes = []
        
        # Crisis detection based on specific phrases
        crisis_phrases = [
            'kill myself', 'want to die', 'end it all', 'suicidal',
            'self harm', 'cutting myself', 'overdose', 'no reason to live'
        ]
        
        has_crisis_phrase = any(phrase in original_text.lower() for phrase in crisis_phrases)
        
        if has_crisis_phrase:
            return {
                'label': 'CRISIS',
                'severity': 'HIGH',
                'confidence': 0.95,
                'needs_attention': True,
                'risk_level': 'HIGH',
                'notes': ['Crisis phrase detected']
            }
        
        # Enhanced classification with mental health sensitivity
        if context['is_mental_health']:
            if score <= -0.4:
                label = 'NEGATIVE'
                severity = 'HIGH'
                confidence = min(0.9, abs(score) + 0.2)
                needs_attention = True
                risk_level = 'HIGH'
                notes.append('Strong negative mental health content')
            elif score <= -0.15:
                label = 'NEGATIVE'
                severity = 'MEDIUM'
                confidence = min(0.8, abs(score) + 0.15)
                needs_attention = True
                risk_level = 'MEDIUM'
                notes.append('Negative mental health content')
            elif score >= 0.4:
                label = 'POSITIVE'
                severity = 'HIGH'
                confidence = min(0.9, score + 0.2)
                needs_attention = False
                risk_level = 'LOW'
                notes.append('Strong positive mental health content')
            elif score >= 0.15:
                label = 'POSITIVE'
                severity = 'MEDIUM'
                confidence = min(0.8, score + 0.15)
                needs_attention = False
                risk_level = 'LOW'
                notes.append('Positive mental health content')
            else:
                label = 'NEUTRAL'
                severity = 'LOW'
                confidence = 0.6
                needs_attention = False
                risk_level = 'LOW'
                notes.append('Neutral mental health content')
        else:
            # Standard classification for general content
            if score <= -0.1:
                label = 'NEGATIVE'
                severity = 'MEDIUM'
                confidence = min(0.8, abs(score) + 0.1)
                needs_attention = False
                risk_level = 'LOW'
            elif score >= 0.1:
                label = 'POSITIVE'
                severity = 'MEDIUM'
                confidence = min(0.8, score + 0.1)
                needs_attention = False
                risk_level = 'LOW'
            else:
                label = 'NEUTRAL'
                severity = 'LOW'
                confidence = 0.7
                needs_attention = False
                risk_level = 'LOW'
        
        return {
            'label': label,
            'severity': severity,
            'confidence': confidence,
            'needs_attention': needs_attention,
            'risk_level': risk_level,
            'notes': notes
        }

    def _default_sentiment(self):
        return {
            'sentiment_label': 'NEUTRAL',
            'sentiment_score': 0,
            'original_score': 0,
            'confidence': 0,
            'severity': 'LOW',
            'mental_health_context': {
                'has_any_context': False,
                'mental_health_score': 0,
                'positive_context_score': 0,
                'emoticon_score': 0
            },
            'needs_attention': False,
            'risk_level': 'LOW',
            'analysis_notes': ['Insufficient text for analysis']
        }