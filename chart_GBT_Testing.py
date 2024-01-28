from chatGPT_function import get_gpt_response as GPT_promt

prompt = input("Kindly ask your question.." )
# prompt = "limiti Audience"
response = GPT_promt(prompt)
print(response)