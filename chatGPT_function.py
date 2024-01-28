
def get_gpt_response(prompt, max_tokens = 200): 
    import openai
    import os
    from dotenv import load_dotenv
    load_dotenv()
    openai.api_key = os.getenv("gbt_key")
    prompt = f" Answer the following question: {prompt}\nAnswer:"
    response = openai.Completion.create( 
        # engine = "text-davinci-003",
        engine = "gpt-3.5-turbo-instruct",
        prompt = prompt,
        max_tokens=500, 
        n=1, 
        temperature = 0.7)
    return response. choices[0].text.strip()