import random
from datetime import datetime

class MentalHealthResponder:
    def __init__(self):
        self.responses = {
            'CRISIS': [
                "🚨 This sounds very serious. Please reach out to a mental health professional immediately. You can contact a crisis helpline - they're available 24/7 and want to help you.",
                "🚨 I'm very concerned about what you're sharing. Please call a crisis helpline right now. Your life is precious and there are people who want to support you.",
                "🚨 This is an emergency situation. Please contact emergency services or a crisis hotline immediately. You don't have to go through this alone."
            ],
            'NEGATIVE_HIGH': [
                "That sounds incredibly difficult to deal with. Have you considered speaking with a therapist or counselor? Professional support can make a big difference.",
                "I hear how much you're struggling. Remember that these feelings, while overwhelming, can get better with the right support.",
                "It takes courage to share these feelings. Many people find relief through therapy, support groups, or talking with trusted friends."
            ],
            'NEGATIVE_MEDIUM': [
                "I'm sorry you're going through this. It might help to talk to someone you trust about how you're feeling.",
                "That sounds challenging. Remember to be kind to yourself - what you're experiencing is valid and many people go through similar struggles.",
                "Thank you for sharing. Sometimes just expressing these feelings can be a first step toward feeling better."
            ],
            'POSITIVE_HIGH': [
                "That's wonderful to hear! It's great that you're experiencing positive moments in your mental health journey.",
                "I'm so glad you're feeling good! Celebrating these positive steps is important for continued progress.",
                "That's fantastic! Remember these positive feelings when things get challenging - progress isn't always linear."
            ],
            'POSITIVE_MEDIUM': [
                "That's great! Small positive steps are still progress worth celebrating.",
                "I'm happy to hear you're doing well! Maintaining mental wellness is an ongoing journey.",
                "That's positive news! Recognizing and appreciating these moments is important."
            ],
            'NEUTRAL_MENTAL_HEALTH': [
                "Thanks for sharing. Checking in with yourself is an important part of mental wellness.",
                "It's good to be aware of your mental state, even when things feel neutral.",
                "Being mindful of how you're feeling, even when it's neutral, shows good self-awareness."
            ],
            'GENERAL_POSITIVE': [
                "That's great to hear! 😊",
                "Wonderful! Positive energy is contagious!",
                "Awesome! Keep that positive mindset going!"
            ],
            'GENERAL_NEGATIVE': [
                "I'm sorry to hear that. Hope things get better soon.",
                "That sounds tough. Sending positive thoughts your way.",
                "I understand. Sometimes we all have difficult moments."
            ]
        }

    def generate_response(self, sentiment_data, username):
        """Generate an appropriate response based on sentiment analysis"""
        label = sentiment_data.get('sentiment_label', 'NEUTRAL')
        severity = sentiment_data.get('severity', 'LOW')
        is_mental_health = sentiment_data.get('mental_health_context', {}).get('has_any_context', False)
        
        response_key = self._get_response_key(label, severity, is_mental_health)
        responses = self.responses.get(response_key, ["Thanks for sharing."])
        
        response_text = random.choice(responses)
        
        return {
            'username': 'MentalHealthBot',
            'message': response_text,
            'timestamp': datetime.now().isoformat(),
            'type': 'bot_response',
            'response_to': username,
            'sentiment_label': label,
            'response_category': response_key
        }

    def _get_response_key(self, label, severity, is_mental_health):
        """Determine the appropriate response category"""
        if label == 'CRISIS':
            return 'CRISIS'
        elif label == 'NEGATIVE':
            if severity == 'HIGH':
                return 'NEGATIVE_HIGH'
            else:
                return 'NEGATIVE_MEDIUM'
        elif label == 'POSITIVE':
            if severity == 'HIGH':
                return 'POSITIVE_HIGH'
            else:
                return 'POSITIVE_MEDIUM'
        elif is_mental_health:
            return 'NEUTRAL_MENTAL_HEALTH'
        elif label == 'POSITIVE':
            return 'GENERAL_POSITIVE'
        elif label == 'NEGATIVE':
            return 'GENERAL_NEGATIVE'
        else:
            return 'NEUTRAL_MENTAL_HEALTH'